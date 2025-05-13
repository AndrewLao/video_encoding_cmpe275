import asyncio
import grpc
import replication_pb2
import replication_pb2_grpc
import argparse
import logging
import os
import time
import uuid
import tempfile
import subprocess
import glob
from concurrent import futures
from typing import Dict, List, Any, Tuple, Optional, AsyncIterator
import shutil
import sys
import psutil
import ffmpeg
import random

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

SHARDS_DIR = "video_shards"
MASTER_DATA_DIR = "master_data"
MASTER_RETRIEVED_SHARDS_DIR = os.path.join(MASTER_DATA_DIR, "retrieved_shards")
muxer_map = {
    'mp4':  'mp4',
    'mkv':  'matroska',
    'webm': 'webm',
    'mov':  'mov',      # or 'mp4' if your build prefers
}

os.makedirs(SHARDS_DIR, exist_ok=True)
os.makedirs(MASTER_DATA_DIR, exist_ok=True)
os.makedirs(MASTER_RETRIEVED_SHARDS_DIR, exist_ok=True)
logging.info(f"Ensured shards directory exists at: {os.path.abspath(SHARDS_DIR)}")
logging.info(f"Ensured master data directory exists at: {os.path.abspath(MASTER_DATA_DIR)}")
logging.info(f"Ensured master retrieved shards directory exists at: {os.path.abspath(MASTER_RETRIEVED_SHARDS_DIR)}")

STREAM_CHUNK_SIZE = 1024 * 1024

MAX_GRPC_MESSAGE_LENGTH = 1024 * 1024 * 1024  # 1gb

class Node:
    def __init__(self, host: str, port: int, role: str, master_address: Optional[str], known_nodes: List[str]):
        self.host = host
        self.port = port
        self.address = f"{host}:{port}"
        self.role = role
        self.id = str(uuid.uuid4())

        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.leader_address: Optional[str] = None
        self.election_timeout = random.uniform(10, 15)
        self.last_heartbeat_time = time.monotonic()
        self.state = "follower"

        # Store references to background tasks for cancellation
        self._background_tasks: List[asyncio.Task] = []
        self._election_task: Optional[asyncio.Task] = None
        self._pre_election_delay_task: Optional[asyncio.Task] = None
        self._master_announcement_task: Optional[asyncio.Task] = None
        self._other_nodes_health_check_task: Optional[asyncio.Task] = None
        self._master_health_check_task: Optional[asyncio.Task] = None


        self.video_statuses: Dict[str, Dict[str, Any]] = {}

        self.processing_tasks: Dict[str, asyncio.Task] = {}
        self._unreported_processed_shards: Dict[Tuple[str, str], str] = {}

        self._server: Optional[grpc.aio.Server] = None
        self._channels: Dict[str, grpc.aio.Channel] = {}
        self._node_stubs: Dict[str, replication_pb2_grpc.NodeServiceStub] = {}
        self._worker_stubs: Dict[str, replication_pb2_grpc.WorkerServiceStub] = {}

        self.master_stub: Optional[replication_pb2_grpc.MasterServiceStub] = None
        self._master_channel: Optional[grpc.ServiceChannel] = None
        self._master_channel_address: Optional[str] = None


        self._master_service_added = False
        self._worker_service_added = False

        self.known_nodes = list(set(known_nodes))
        if self.address in self.known_nodes:
             self.known_nodes.remove(self.address)

        self.current_master_address = master_address

        logging.info(f"[{self.address}] Starting as {self.role.upper()}. Explicit master: {master_address}")

        for node_addr in self.known_nodes:
             if node_addr != self.address:
                self._create_stubs_for_node(node_addr)

        if self.role == 'worker' and self.current_master_address:
             logging.info(f"[{self.address}] Creating/Updating MasterService stubs for {self.current_master_address}")
             self._create_master_stubs(self.current_master_address)


        logging.info(f"[{self.address}] Initialized as {self.role.upper()}. Master is {self.current_master_address}. My ID: {self.id}. Current Term: {self.current_term}")

    def _get_or_create_channel(self, node_address: str) -> grpc.aio.Channel:
        """Gets an existing channel or creates a new one."""
        if node_address not in self._channels or (self._channels.get(node_address) and self._channels[node_address]._channel.closed()):
             logging.info(f"[{self.address}] Creating new channel for {node_address} with max message size {MAX_GRPC_MESSAGE_LENGTH} bytes")
             self._channels[node_address] = grpc.aio.insecure_channel(
                 node_address,
                 options=[
                     ('grpc.max_send_message_length', MAX_GRPC_MESSAGE_LENGTH),
                     ('grpc.max_receive_message_length', MAX_GRPC_MESSAGE_LENGTH),
                 ]
             )
        return self._channels[node_address]

    def _create_stubs_for_node(self, node_address: str):
        """Creates NodeService and potentially WorkerService stubs."""
        channel = self._get_or_create_channel(node_address)
        self._node_stubs[node_address] = replication_pb2_grpc.NodeServiceStub(channel)
        if self.role == 'master':
            self._worker_stubs[node_address] = replication_pb2_grpc.WorkerServiceStub(channel)
            logging.info(f"[{self.address}] Created WorkerService stub for {node_address}")
        logging.info(f"[{self.address}] Created NodeService stub for {node_address}")


    def _create_master_stubs(self, master_address: str):
        """Creates or updates stubs for the master node's services."""
        master_channel_valid = self._master_channel and self._master_channel_address == master_address and not self._master_channel._channel.closed()

        if master_channel_valid:
             logging.debug(f"[{self.address}] Existing master channels to {master_address} are valid.")
             return

        if self._master_channel:
             logging.info(f"[{self.address}] Closing old master MasterService channel to {self._master_channel_address}")
             asyncio.create_task(self._master_channel.close())
             self._master_channel = None
             self._master_channel_address = None


        logging.info(f"[{self.address}] Creating new master channels for {master_address} with max message size {MAX_GRPC_MESSAGE_LENGTH} bytes")

        self._master_channel = grpc.aio.insecure_channel(
            master_address,
            options=[
                ('grpc.max_send_message_length', MAX_GRPC_MESSAGE_LENGTH),
                ('grpc.max_receive_message_length', MAX_GRPC_MESSAGE_LENGTH),
            ]
        )
        self._master_channel_address = master_address
        self.master_stub = replication_pb2_grpc.MasterServiceStub(self._master_channel)
        logging.info(f"[{self.address}] MasterService stub updated for {master_address}")


    def _get_or_create_master_stub(self) -> Optional[replication_pb2_grpc.MasterServiceStub]:
        """Returns the MasterService stub for the current master."""
        current_master_address = self.current_master_address
        if not current_master_address:
             return None
        self._create_master_stubs(current_master_address)
        return self.master_stub


    async def start(self):
        """Starts the gRPC server and background routines."""
        server_options = [
            ('grpc.max_send_message_length', MAX_GRPC_MESSAGE_LENGTH),
            ('grpc.max_receive_message_length', MAX_GRPC_MESSAGE_LENGTH),
        ]
        self._server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10), options=server_options)

        replication_pb2_grpc.NodeServiceServicer.__init__(self)
        self._server.add_insecure_port(self.address)
        replication_pb2_grpc.add_NodeServiceServicer_to_server(self, self._server)

        # Add WorkerService unconditionally if the initial role is worker
        if self.role == 'worker':
             replication_pb2_grpc.WorkerServiceServicer.__init__(self)
             replication_pb2_grpc.add_WorkerServiceServicer_to_server(self, self._server)
             self._worker_service_added = True
             logging.info(f"[{self.address}] WorkerServiceServicer added to server.")


        logging.info(f"[{self.address}] Server starting at {self.address} as {self.role.upper()} with max message size {MAX_GRPC_MESSAGE_LENGTH} bytes")
        await self._server.start()
        logging.info(f"[{self.address}] Server started.")

        logging.info(f"[{self.address}] Performing startup master discovery...")
        discovered_master_address: Optional[str] = None
        highest_term_found = self.current_term

        # Query known nodes for their stats to find potential master
        discovery_tasks = []
        for node_addr in self.known_nodes:
             if node_addr != self.address:
                 node_stub = self._node_stubs.get(node_addr)
                 if node_stub:
                     discovery_tasks.append(asyncio.create_task(self._query_node_for_master(node_stub, node_addr)))

        if discovery_tasks:
             done, pending = await asyncio.wait(discovery_tasks, timeout=5) # Short timeout for startup discovery

             for task in done:
                 try:
                     node_addr, is_master, term = task.result()
                     if is_master and term >= highest_term_found:
                         highest_term_found = term
                         discovered_master_address = node_addr
                         logging.info(f"[{self.address}] Discovered potential master at {node_addr} with term {term}.")
                 except Exception as e:
                     logging.error(f"[{self.address}] Error processing discovery task result: {type(e).__name__} - {e}")

             for task in pending:
                 task.cancel()

        if discovered_master_address and highest_term_found >= self.current_term:
             logging.info(f"[{self.address}] Discovered active master {discovered_master_address} with term {highest_term_found}. Transitioning to follower state.")
             self.state = "follower"
             self.role = 'worker' # Assume node becomes worker if not the elected master
             self.current_term = highest_term_found
             self.voted_for = None
             self.current_master_address = discovered_master_address
             self.leader_address = discovered_master_address
             self.last_heartbeat_time = time.monotonic()

             logging.info(f"[{self.address}] Ensuring master stubs are created for {self.current_master_address} and starting health check routine.")
             self._create_master_stubs(self.current_master_address)
             self._master_health_check_task = asyncio.create_task(self.check_master_health())
             self._background_tasks.append(self._master_health_check_task)


        else:
             logging.info(f"[{self.address}] No active master found with term >= my current term during startup discovery. Proceeding with initial role.")
             if self.role == 'master':
                 replication_pb2_grpc.MasterServiceServicer.__init__(self)
                 replication_pb2_grpc.add_MasterServiceServicer_to_server(self, self._server)
                 self._master_service_added = True
                 logging.info(f"[{self.address}] MasterServiceServicer added to server.")

                 # Master also needs WorkerService to receive processed shards from workers
                 if not self._worker_service_added: # Check if already added if started as worker and became master
                     replication_pb2_grpc.WorkerServiceServicer.__init__(self)
                     replication_pb2_grpc.add_WorkerServiceServicer_to_server(self, self._server)
                     self._worker_service_added = True
                     logging.info(f"[{self.address}] WorkerServiceServicer added to server.")


                 logging.info(f"[{self.address}] Initializing worker stubs based on known nodes.")
                 self._worker_stubs = {}
                 for node_addr in self.known_nodes:
                     if node_addr != self.address:
                         self._create_stubs_for_node(node_addr)

                 logging.info(f"[{self.address}] Starting master announcement routine.")
                 self._master_announcement_task = asyncio.create_task(self._master_election_announcement_routine())
                 self._background_tasks.append(self._master_announcement_task)
                 self._other_nodes_health_check_task = asyncio.create_task(self._check_other_nodes_health())
                 self._background_tasks.append(self._other_nodes_health_check_task)


             elif self.role == 'worker':
                 # WorkerServiceServicer is now added unconditionally above if role is worker.
                 # Start worker health check routine.
                 logging.info(f"[{self.address}] Starting worker health check routine.")
                 self._master_health_check_task = asyncio.create_task(self.check_master_health())
                 self._background_tasks.append(self._master_health_check_task)


        logging.info(f"[{self.address}] Node is now running with state: {self.state}, role: {self.role}, current_term: {self.current_term}, master: {self.current_master_address}")

        # Wait for the server to terminate
        await self._server.wait_for_termination()

    async def _query_node_for_master(self, node_stub: replication_pb2_grpc.NodeServiceStub, node_address: str) -> Tuple[str, bool, int]:
        """Queries a node for its master status and term."""
        try:
            logging.debug(f"[{self.address}] Checking node {node_address} for master status.")
            response = await asyncio.wait_for(
                node_stub.GetNodeStats(replication_pb2.NodeStatsRequest()),
                timeout=2
            )
            logging.debug(f"[{self.address}] Received stats from {node_address}. Is Master: {response.is_master}, Term: {response.current_term}")
            return node_address, response.is_master, response.current_term
        except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
            logging.debug(f"[{self.address}] Node {node_address} unresponsive during startup discovery: {e}")
            return node_address, False, -1 # Indicate not master and invalid term
        except Exception as e:
            logging.error(f"[{self.address}] Unexpected error during startup discovery check for {node_address}: {type(e).__name__} - {e}", exc_info=True)
            return node_address, False, -1


    async def stop(self):
        """Shuts down the gRPC server and cancels background tasks."""
        logging.info(f"[{self.address}] Initiating graceful shutdown.")

        # Cancel all background tasks
        logging.info(f"[{self.address}] Cancelling {len(self._background_tasks)} background tasks.")
        for task in self._background_tasks:
             if not task.done():
                task.cancel()

        # Wait for background tasks to complete their cancellation
        await asyncio.gather(*self._background_tasks, return_exceptions=True)
        logging.info(f"[{self.address}] Background tasks cancellation attempted.")

        # Cancel processing tasks (for workers)
        processing_task_list = list(self.processing_tasks.values())
        logging.info(f"[{self.address}] Cancelling {len(processing_task_list)} processing tasks.")
        for task in processing_task_list:
             if not task.done():
                task.cancel()

        await asyncio.gather(*processing_task_list, return_exceptions=True)
        logging.info(f"[{self.address}] Processing tasks cancellation attempted.")

        # Shut down the gRPC server
        if self._server:
             logging.info(f"[{self.address}] Shutting down gRPC server...")
             await self._server.stop(5) # Graceful shutdown with a timeout
             logging.info(f"[{self.address}] gRPC server shut down.")

        # Close all channels
        logging.info(f"[{self.address}] Closing gRPC channels.")
        channel_close_tasks = []
        for address, channel in self._channels.items():
             if channel and not channel._channel.closed():
                 logging.info(f"[{self.address}] Closing channel to {address}")
                 channel_close_tasks.append(asyncio.create_task(channel.close()))

        if self._master_channel and not self._master_channel._channel.closed():
             logging.info(f"[{self.address}] Closing master channel to {self._master_channel_address}")
             channel_close_tasks.append(asyncio.create_task(self._master_channel.close()))

        if channel_close_tasks:
            await asyncio.gather(*channel_close_tasks, return_exceptions=True)
        logging.info(f"[{self.address}] gRPC channels closed.")


        logging.info(f"[{self.address}] Node shutdown complete.")

    async def AnnounceMaster(self, request: replication_pb2.MasterAnnouncement, context: grpc.aio.ServicerContext) -> replication_pb2.MasterAnnouncementResponse:
        """Handles incoming MasterAnnouncement RPCs."""
        logging.info(f"[{self.address}] Received MasterAnnouncement from {context.peer()}. New master is {request.master_address} (ID: {request.node_id_of_master}, Term: {request.term})")

        if request.term > self.current_term:
             logging.info(f"[{self.address}] Received MasterAnnouncement with higher term ({request.term} > {self.current_term}). Updating term and reverting to follower.")
             self.current_term = request.term
             self.state = "follower"
             self.voted_for = None
             self.current_master_address = request.master_address
             self.leader_address = request.master_address # Assuming master is leader in this model
             self.last_heartbeat_time = time.monotonic()
             self.role = 'worker' # A non-master receiving higher term becomes worker

             logging.info(f"[{self.address}] Updating Master stubs to point to {request.master_address} and attempting to report unreported shards.")
             self._create_master_stubs(request.master_address)
             asyncio.create_task(self._attempt_report_unreported_shards())

             # Cancel any pending election/announcement tasks as we are now a follower
             if self._election_task and not self._election_task.done():
                  logging.info(f"[{self.address}] Cancelling pending election task due to new master announcement.")
                  self._election_task.cancel()
                  self._election_task = None
             if self._pre_election_delay_task and not self._pre_election_delay_task.done():
                  logging.info(f"[{self.address}] Cancelling pending pre-election delay due to new master announcement.")
                  self._pre_election_delay_task.cancel()
                  self._pre_election_delay_task = None
             if self._master_announcement_task and not self._master_announcement_task.done():
                  logging.info(f"[{self.address}] Cancelling master announcement task.")
                  self._master_announcement_task.cancel()
                  self._master_announcement_task = None
             if self._other_nodes_health_check_task and not self._other_nodes_health_check_task.done():
                  logging.info(f"[{self.address}] Cancelling other nodes health check task.")
                  self._other_nodes_health_check_task.cancel()
                  self._other_nodes_health_check_task = None

             # Ensure master health check is running if we are a worker
             if self.role == 'worker' and (self._master_health_check_task is None or self._master_health_check_task.done()):
                  logging.info(f"[{self.address}] Starting master health check routine.")
                  self._master_health_check_task = asyncio.create_task(self.check_master_health())
                  self._background_tasks.append(self._master_health_check_task)


        elif request.term == self.current_term:
             if self.state == "follower":
                 logging.info(f"[{self.address}] Received MasterAnnouncement with equal term ({request.term}) while in follower state. Accepting as heartbeat.")
                 self.current_master_address = request.master_address
                 self.leader_address = request.master_address
                 self.last_heartbeat_time = time.monotonic()
                 if self.role == 'worker':
                    logging.info(f"[{self.address}] Updating Master stubs to point to {request.master_address} and attempting to report unreported shards.")
                    self._create_master_stubs(request.master_address)
                    asyncio.create_task(self._attempt_report_unreported_shards())

             elif self.state == "candidate":
                 logging.warning(f"[{self.address}] Received MasterAnnouncement with equal term ({request.term}) while in candidate state. Stepping down to follower.")
                 self.state = "follower"
                 self.voted_for = None
                 self.current_master_address = request.master_address
                 self.leader_address = request.master_address
                 self.last_heartbeat_time = time.monotonic()
                 self.role = 'worker'

                 logging.info(f"[{self.address}] Updating Master stubs to point to {request.master_address} and attempting to report unreported shards.")
                 self._create_master_stubs(request.master_address)
                 asyncio.create_task(self._attempt_report_unreported_shards())

                 if self._pre_election_delay_task and not self._pre_election_delay_task.done():
                      logging.info(f"[{self.address}] Cancelling pending pre-election delay due to stepping down from candidate.")
                      self._pre_election_delay_task.cancel()
                      self._pre_election_delay_task = None
                 if self._election_task and not self._election_task.done():
                      logging.info(f"[{self.address}] Cancelling pending election task.")
                      self._election_task.cancel()
                      self._election_task = None

                 # Ensure master health check is running if we are a worker
                 if self.role == 'worker' and (self._master_health_check_task is None or self._master_health_check_task.done()):
                      logging.info(f"[{self.address}] Starting master health check routine.")
                      self._master_health_check_task = asyncio.create_task(self.check_master_health())
                      self._background_tasks.append(self._master_health_check_task)


             elif self.state == "leader":
                 logging.warning(f"[{self.address}] Received MasterAnnouncement with equal term ({request.term}) while in leader state. Potential split-brain. Ignoring announcement and continuing as leader.")

        elif request.term < self.current_term:
             if self.current_master_address is not None:
                 logging.info(f"[{self.address}] Received MasterAnnouncement with lower term ({request.term} < {self.current_term}). Ignoring.")
             else:
                 logging.warning(f"[{self.address}] Received MasterAnnouncement with lower term ({request.term} < {self.current_term}) but I don't know a master. Accepting for now to discover a master.")
                 self.current_term = request.term
                 self.state = "follower"
                 self.voted_for = None
                 self.current_master_address = request.master_address
                 self.leader_address = request.master_address # Assuming master is leader in this model
                 self.last_heartbeat_time = time.monotonic()
                 self.role = 'worker'

                 logging.info(f"[{self.address}] Updating Master stubs to point to {request.master_address} and attempting to report unreported shards.")
                 self._create_master_stubs(request.master_address)
                 asyncio.create_task(self._attempt_report_unreported_shards())

                 if self._pre_election_delay_task and not self._pre_election_delay_task.done():
                      logging.info(f"[{self.address}] Cancelling pending pre-election delay due to accepting lower term announcement (no master known).")
                      self._pre_election_delay_task.cancel()
                      self._pre_election_delay_task = None

                 # Ensure master health check is running if we are a worker
                 if self.role == 'worker' and (self._master_health_check_task is None or self._master_health_check_task.done()):
                      logging.info(f"[{self.address}] Starting master health check routine.")
                      self._master_health_check_task = asyncio.create_task(self.check_master_health())
                      self._background_tasks.append(self._master_health_check_task)


        response = replication_pb2.MasterAnnouncementResponse(
            status=f"Acknowledged by {self.id}",
            node_id=self.id
        )
        return response

    async def RequestVote(self, request: replication_pb2.VoteRequest, context: grpc.aio.ServicerContext) -> replication_pb2.VoteResponse:
        """Handles incoming VoteRequest RPCs."""
        logging.info(f"[{self.address}] Received VoteRequest from {context.peer()} (Candidate ID: {request.candidate_id}, Address: {request.candidate_address}, Term: {request.term})")

        vote_granted = False
        if request.term > self.current_term:
             logging.info(f"[{self.address}] Received VoteRequest with higher term ({request.term} > {self.current_term}). Updating term and voting for {request.candidate_id}.")
             self.current_term = request.term
             self.state = "follower"
             self.voted_for = request.candidate_id
             vote_granted = True
             self.last_heartbeat_time = time.monotonic()
             self.role = 'worker'

             # Cancel any pending election/announcement tasks as we are now a follower
             if self._election_task and not self._election_task.done():
                  logging.info(f"[{self.address}] Cancelling pending election task due to granting vote for higher term.")
                  self._election_task.cancel()
                  self._election_task = None
             if self._pre_election_delay_task and not self._pre_election_delay_task.done():
                  logging.info(f"[{self.address}] Cancelling pending pre-election delay due to granting vote for higher term.")
                  self._pre_election_delay_task.cancel()
                  self._pre_election_delay_task = None
             if self._master_announcement_task and not self._master_announcement_task.done():
                  logging.info(f"[{self.address}] Cancelling master announcement task.")
                  self._master_announcement_task.cancel()
                  self._master_announcement_task = None
             if self._other_nodes_health_check_task and not self._other_nodes_health_check_task.done():
                  logging.info(f"[{self.address}] Cancelling other nodes health check task.")
                  self._other_nodes_health_check_task.cancel()
                  self._other_nodes_health_check_task = None

             # Ensure master health check is running if we are a worker
             if self.role == 'worker' and (self._master_health_check_task is None or self._master_health_check_task.done()):
                  logging.info(f"[{self.address}] Starting master health check routine.")
                  self._master_health_check_task = asyncio.create_task(self.check_master_health())
                  self._background_tasks.append(self._master_health_check_task)


        elif request.term == self.current_term:
             if self.voted_for is None or self.voted_for == request.candidate_id:
                logging.info(f"[{self.address}] Received VoteRequest with equal term ({request.term}). Granting vote to {request.candidate_id}.")
                self.voted_for = request.candidate_id
                vote_granted = True
                self.last_heartbeat_time = time.monotonic()

                if self._pre_election_delay_task and not self._pre_election_delay_task.done():
                     logging.info(f"[{self.address}] Cancelling pending pre-election delay due to granting vote for equal term.")
                     self._pre_election_delay_task.cancel()
                     self._pre_election_delay_task = None

             else:
                logging.info(f"[{self.address}] Received VoteRequest with equal term ({request.term}) but already voted for {self.voted_for}. Denying vote to {request.candidate_id}.")

        else:
             logging.info(f"[{self.address}] Received VoteRequest with lower term ({request.term} < {self.current_term}). Denying vote to {request.candidate_id}.")

        response = replication_pb2.VoteResponse(
            vote_granted=vote_granted,
            voter_id=self.id,
            voter_address=self.address
        )
        return response

    async def GetNodeStats(self, request: replication_pb2.NodeStatsRequest, context: grpc.aio.ServicerContext) -> replication_pb2.NodeStatsResponse:
        """Provides statistics about the node."""
        logging.debug(f"[{self.address}] Received GetNodeStats request from {context.peer()}")
        cpu_percent = psutil.cpu_percent(interval=1)
        memory_info = psutil.virtual_memory()
        memory_percent = memory_info.percent

        try:
             shards_disk_usage = shutil.disk_usage(SHARDS_DIR)
             disk_space_free_shards = shards_disk_usage.free
             disk_space_total_shards = shards_disk_usage.total
        except Exception:
             disk_space_free_shards = -1
             disk_space_total_shards = -1

        try:
             master_data_disk_usage = shutil.disk_usage(MASTER_DATA_DIR)
             disk_space_free_masterdata = master_data_disk_usage.free
             disk_space_total_masterdata = master_data_disk_usage.total
        except Exception:
             disk_space_free_masterdata = -1
             disk_space_total_masterdata = -1

        response = replication_pb2.NodeStatsResponse(
            node_id=self.id,
            node_address=self.address,
            is_master=(self.role == 'master'),
            current_master_address=self.current_master_address if self.current_master_address else "",
            cpu_utilization=cpu_percent,
            memory_utilization=memory_percent,
            disk_space_free_shards=disk_space_free_shards,
            disk_space_total_shards=disk_space_total_shards,
            disk_space_free_masterdata=disk_space_free_masterdata,
            disk_space_total_masterdata=disk_space_total_masterdata,
            active_tasks=len(self.processing_tasks) if self.role == 'worker' else len([task for task in asyncio.all_tasks() if task is not asyncio.current_task()]),
            known_nodes_count=len(self.known_nodes) + 1,
            election_in_progress=(self.state in ["candidate", "leader"]),
            current_term=self.current_term
        )
        return response

    async def GetCurrentMaster(self, request: replication_pb2.GetCurrentMasterRequest, context: grpc.aio.ServicerContext) -> replication_pb2.GetCurrentMasterResponse:
        """Provides the address and term of the current master."""
        logging.debug(f"[{self.address}] Received GetCurrentMaster request from {context.peer()}")
        return replication_pb2.GetCurrentMasterResponse(
            master_address=self.current_master_address if self.current_master_address else "",
            term=self.current_term,
            is_master_known=self.current_master_address is not None
        )


    async def UploadVideo(self, request_iterator: AsyncIterator[replication_pb2.UploadVideoChunk], context: grpc.aio.ServicerContext) -> replication_pb2.UploadVideoResponse:
        """Handles video upload requests (master only)."""
        if self.role != 'master':
             return replication_pb2.UploadVideoResponse(success=False, message="This node is not the master.")

        logging.info(f"[{self.address}] Received UploadVideo stream request.")

        video_id: Optional[str] = None
        target_width: Optional[int] = None
        target_height: Optional[Optional[int]] = None
        original_filename: Optional[str] = None
        temp_input_path: Optional[str] = None

        try:
            first_chunk = await anext(request_iterator)
            if not first_chunk.is_first_chunk:
                raise ValueError("First chunk in UploadVideo stream must have is_first_chunk set to True.")

            video_id           = first_chunk.video_id or str(uuid.uuid4())
            target_width       = first_chunk.target_width
            target_height      = first_chunk.target_height
            original_filename  = first_chunk.original_filename or f"{video_id}.mp4"

           
            upscale_width      = first_chunk.upscale_width  or target_width
            upscale_height     = first_chunk.upscale_height or target_height
            container          = first_chunk.output_format or 'mp4'

            # Decide container & codec based on requested format
            vcodec    = 'libx264'     if container in ('mp4','mov','mkv') else 'libvpx-vp9'
            acodec    = 'aac'         if container in ('mp4','mov','mkv') else 'libvorbis'
            
            logging.info(f"[{self.address}] Received metadata for video ID: {video_id}")

            temp_input_path = os.path.join(MASTER_DATA_DIR, f"{video_id}_original.tmp")
            # Use run_in_executor for blocking file write
            loop = asyncio.get_event_loop()
            with open(temp_input_path, 'wb') as f:
                await loop.run_in_executor(None, f.write, first_chunk.data_chunk)
                async for chunk_message in request_iterator:
                    if chunk_message.is_first_chunk:
                         logging.warning(f"[{self.address}] Received unexpected first chunk indicator for video ID: {video_id} in subsequent message.")
                    await loop.run_in_executor(None, f.write, chunk_message.data_chunk)

            logging.info(f"[{self.address}] Finished receiving all chunks for video ID: {video_id}. File saved to {temp_input_path}")

            self.video_statuses[video_id] = {
                 "status": "segmenting",
                 "container": container, 
                 "target_width": target_width,
                 "target_height": target_height,
                 "original_filename": original_filename,
                 "shards": {},
                 "retrieved_shards": {},
                 "concatenation_task": None
            }

           

            # Build output pattern (use the container extension)
            output_pattern = os.path.join(
                MASTER_DATA_DIR,
                f"{video_id}_shard_%04d.{container}"
            )
            segment_time = 10  # or make it another parameter

            try:
                logging.info(
                    f"[{self.address}] Starting segmentation for video {video_id} from {temp_input_path}"
                )
                # FFmpeg is a blocking process, run in executor
                await loop.run_in_executor(
                    None,
                    lambda: (
                        ffmpeg
                        .input(temp_input_path)
                        # apply scaling up/down to the requested upscale dimensions
                        .filter("scale", upscale_width, upscale_height)
                        # segment muxer
                        .output(
                            output_pattern,
                            format="segment",
                            segment_time=segment_time,
                            segment_format_options="fflags=+genpts",
                            reset_timestamps=1,
                            force_key_frames="expr:gte(t,n_forced*10)",
                            vcodec=vcodec,
                            acodec=acodec,
                            **({"video_bitrate": "2M"} if vcodec == "libx264" else {}),
                            write_prft=1,
                        )
                        .run(
                            capture_stdout=True,
                            capture_stderr=True,
                            overwrite_output=True
                        )
                    )
                )
                logging.info(f"[{self.address}] Successfully segmented video {video_id}")
                self.video_statuses[video_id]["status"] = "segmented"

                shard_files = sorted(
                    glob.glob(
                        os.path.join(MASTER_DATA_DIR, f"{video_id}_shard_*.{container}")
                    )
                )
                logging.info(f"[{self.address}] Found {len(shard_files)} shards for video {video_id}")

                if not shard_files:
                    raise Exception("No video segments were created by FFmpeg.")

                self.video_statuses[video_id]["total_shards"] = len(shard_files)

                # Distribute shards as a background task
                distribute_task = asyncio.create_task(
                    self._distribute_shards(
                        video_id, shard_files, target_width, target_height, original_filename
                    )
                )
                self._background_tasks.append(distribute_task)

                return replication_pb2.UploadVideoResponse(
                    video_id=video_id,
                    success=True,
                    message="Video uploaded and segmentation started."
                )

            except ffmpeg.Error as e:
                 logging.error(f"[{self.address}] FFmpeg segmentation failed for {video_id}: {e.stderr.decode()}", exc_info=True)
                 self.video_statuses[video_id]["status"] = "failed_segmentation"
                 self.video_statuses[video_id]["message"] = f"FFmpeg segmentation failed: {e.stderr.decode()}"
                 return replication_pb2.UploadVideoResponse(video_id=video_id, success=False, message=f"FFmpeg segmentation failed: {e.stderr.decode()}")
            except Exception as e:
                 logging.error(f"[{self.address}] Segmentation failed for {video_id}: {type(e).__name__} - {e}", exc_info=True)
                 self.video_statuses[video_id]["status"] = "failed_segmentation"
                 self.video_statuses[video_id]["message"] = f"Segmentation failed: {type(e).__name__} - {e}"
                 return replication_pb2.UploadVideoResponse(video_id=video_id, success=False, message=f"Segmentation failed: {type(e).__name__} - {e}")

        except Exception as e:
             logging.error(f"[{self.address}] Error during UploadVideo stream processing for video ID {video_id}: {type(e).__name__} - {e}", exc_info=True)
             if temp_input_path and os.path.exists(temp_input_path):
                # Use run_in_executor for blocking file removal
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, os.remove, temp_input_path)
                logging.info(f"[{self.address}] Cleaned up partial upload file: {temp_input_path}")
             if video_id and video_id in self.video_statuses:
                  self.video_statuses[video_id]["status"] = "upload_failed"
                  self.video_statuses[video_id]["message"] = f"Upload stream processing failed: {type(e).__name__} - {e}"
             else:
                  logging.error(f"[{self.address}] Generic upload stream processing failed before video ID was determined: {type(e).__name__} - {e}", exc_info=True)

             return replication_pb2.UploadVideoResponse(
                 video_id=video_id if video_id else "unknown",
                 success=False,
                 message=f"Upload stream processing failed: {type(e).__name__} - {e}"
             )
        # finally:
        #      # Clean up the temporary original video file AFTER segmentation
        #      # If segmentation failed, the error handling above already cleans it up.
        #      # If segmentation succeeded, the shards are distributed, and we can remove the original.
        #      if temp_input_path and os.path.exists(temp_input_path) and self.video_statuses.get(video_id, {}).get("status") != "segmenting":
        #          # Only remove if segmentation was attempted or completed.
        #          # If segmentation failed, it's handled in the segmentation error block.
        #          # If segmentation succeeded, the shards are distributed, and we can remove the original.
        #          if self.video_statuses.get(video_id, {}).get("status") in ["segmented", "shards_distributed", "all_shards_processed", "processing_failed", "concatenation_failed", "concatenation_prerequisites_failed", "completed"]:
        #              # Use run_in_executor for blocking file removal
        #              loop = asyncio.get_event_loop()
        #              await loop.run_in_executor(None, os.remove, temp_input_path)
        #              logging.info(f"[{self.address}] Cleaned up original video file: {temp_input_path}")


    async def _distribute_shards(self, video_id: str, shard_files: List[str], target_width: int, target_height: int, original_filename: str):
        """Distributes video shards to available worker nodes."""
        logging.info(f"[{self.address}] Starting distribution of {len(shard_files)} shards for video {video_id}")

        available_worker_addresses = list(self._worker_stubs.keys())
        if not available_worker_addresses:
             logging.error(f"[{self.address}] No workers available to process shards for video {video_id}")
             self.video_statuses[video_id]["status"] = "failed_distribution"
             self.video_statuses[video_id]["message"] = "No workers available."
             return

        total_shards = len(shard_files)
        shards_to_distribute = list(enumerate(shard_files))

        failed_to_distribute_in_round = []
        worker_index = 0

        loop = asyncio.get_event_loop() # Get the event loop for run_in_executor

        while shards_to_distribute and available_worker_addresses:
             shard_index, shard_file = shards_to_distribute.pop(0)
             shard_id = os.path.basename(shard_file)

             worker_found = False
             for i in range(len(available_worker_addresses)):
                 current_worker_index = (worker_index + i) % len(available_worker_addresses)
                 worker_address = available_worker_addresses[current_worker_index]
                 worker_stub = self._worker_stubs.get(worker_address)

                 if not worker_stub:
                    logging.warning(f"[{self.address}] No WorkerService stub for {worker_address}. Removing from available list for this round.")
                    # Remove the worker from the list if the stub is missing
                    if worker_address in available_worker_addresses:
                         available_worker_addresses.remove(worker_address)
                    continue

                 try:
                    # Use run_in_executor for blocking file read
                    shard_data = await loop.run_in_executor(None, self._read_file_blocking, shard_file)

                    request = replication_pb2.DistributeShardRequest(
                         video_id=video_id,
                         shard_id=shard_id,
                         shard_data=shard_data,
                         shard_index=shard_index,
                         total_shards=total_shards,
                         target_width=target_width,
                         target_height=target_height,
                         original_filename=original_filename
                    )

                    logging.info(f"[{self.address}] Sending shard {shard_id} ({len(shard_data)} bytes) to worker {worker_address}")
                    response = await asyncio.wait_for(worker_stub.ProcessShard(request), timeout=30000)

                    if response.success:
                         logging.info(f"[{self.address}] Worker {worker_address} accepted shard {shard_id} for processing.")
                         self.video_statuses[video_id]["shards"][shard_id] = {
                             "status": "sent_to_worker",
                             "worker_address": worker_address,
                             "index": shard_index
                         }
                         # Use run_in_executor for blocking file removal
                         await loop.run_in_executor(None, self._remove_file_blocking, shard_file)
                         worker_found = True
                         worker_index = (current_worker_index + 1) % len(available_worker_addresses)
                         break

                    else:
                         logging.error(f"[{self.address}] Worker {worker_address} rejected shard {shard_id}: {response.message}. Trying next available worker.")

                 except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
                    logging.error(f"[{self.address}] RPC failed or timed out when sending shard {shard_id} to {worker_address}: {e}. Removing worker from available list for this round and trying next available worker.")
                    # Remove the worker from the list if RPC fails/times out
                    if worker_address in available_worker_addresses:
                         available_worker_addresses.remove(worker_address)
                         logging.info(f"[{self.address}] Removed worker {worker_address} from available list for this distribution round.")

                 except Exception as e:
                    logging.error(f"[{self.address}] Failed to send shard {shard_id} to {worker_address}: {type(e).__name__} - {e}. Marking shard as failed distribution.", exc_info=True)
                    self.video_statuses[video_id]["shards"][shard_id] = {
                        "status": "failed_distribution",
                        "worker_address": worker_address,
                        "message": f"Failed to send: {type(e).__name__} - {e}",
                        "index": shard_index
                    }
                    failed_to_distribute_in_round.append((shard_index, shard_file))
                    worker_found = True # Consider this shard attempt finished (failed)
                    # Use run_in_executor for blocking file removal
                    await loop.run_in_executor(None, self._remove_file_blocking, shard_file)
                    break

             if not worker_found:
                 logging.warning(f"[{self.address}] Failed to distribute shard {shard_id} to any available worker in this round. Adding back to the distribution queue.")
                 shards_to_distribute.append((shard_index, shard_file))

        if not shards_to_distribute and not failed_to_distribute_in_round:
             self.video_statuses[video_id]["status"] = "shards_distributed"
             logging.info(f"[{self.address}] Finished attempting to distribute all shards for video {video_id}.")
        else:
             undistributed_count = len(shards_to_distribute) + len(failed_to_distribute_in_round)
             self.video_statuses[video_id]["status"] = "partial_distribution_failed"
             self.video_statuses[video_id]["message"] = f"Failed to distribute {undistributed_count} out of {total_shards} shards."
             logging.error(f"[{self.address}] Partial distribution failed for video {video_id}. {undistributed_count} shards remain undistributed or failed.")
             for _, shard_file in shards_to_distribute:
                 # Use run_in_executor for blocking file removal
                 await loop.run_in_executor(None, self._remove_file_blocking, shard_file)
                 logging.info(f"[{self.address}] Cleaned up remaining temporary shard file: {shard_file}")

    # Helper functions for blocking file operations to be used with run_in_executor
    def _read_file_blocking(self, filepath):
        with open(filepath, 'rb') as f:
            return f.read()

    def _remove_file_blocking(self, filepath):
        if os.path.exists(filepath):
            os.remove(filepath)

    async def ReportWorkerShardStatus(self, request: replication_pb2.ReportWorkerShardStatusRequest, context: grpc.aio.ServicerContext) -> replication_pb2.ReportWorkerShardStatusResponse:
        """Handles status updates from worker nodes (master only)."""
        if self.role != 'master':
            return replication_pb2.ReportWorkerShardStatusResponse(success=False, message="This node is not the master.")

        video_id = request.video_id
        shard_id = request.shard_id
        worker_address = request.worker_address
        status = request.status

        logging.info(f"[{self.address}] Received ReportWorkerShardStatus for video {video_id}, shard {shard_id} from {worker_address} with status: {status}")

        if video_id not in self.video_statuses:
            logging.warning(f"[{self.address}] Received status update for unknown video ID: {video_id}")
            return replication_pb2.ReportWorkerShardStatusResponse(success=False, message=f"Unknown video ID: {video_id}")

        if shard_id in self.video_statuses[video_id]["shards"] and self.video_statuses[video_id]["shards"][shard_id]["status"] in ["failed_sending", "rpc_failed", "failed_distribution"]:
            logging.info(f"[{self.address}] Received status for shard {shard_id} previously marked as failed distribution. Updating status.")
            original_index = self.video_statuses[video_id]["shards"][shard_id].get("index", -1)
            self.video_statuses[video_id]["shards"][shard_id] = {
                "status": status,
                "worker_address": worker_address,
                "index": original_index
            }
        elif shard_id not in self.video_statuses[video_id]["shards"]:
            logging.warning(f"[{self.address}] Received status update for unknown shard ID {shard_id} for video {video_id} that wasn't in the initial distribution list.")
            self.video_statuses[video_id]["shards"][shard_id] = {
                "status": status,
                "worker_address": worker_address,
                "index": -1
            }
        else:
            self.video_statuses[video_id]["shards"][shard_id]["status"] = status
            self.video_statuses[video_id]["shards"][shard_id]["worker_address"] = worker_address

        if status == "processed_successfully":
            # Retrieve processed shard as a background task
            retrieve_task = asyncio.create_task(self._retrieve_processed_shard(video_id, shard_id, worker_address))
            self._background_tasks.append(retrieve_task)

        return replication_pb2.ReportWorkerShardStatusResponse(success=True, message="Status updated.")


    async def _retrieve_processed_shard(self, video_id: str, shard_id: str, worker_address: str):
        """Retrieves a processed shard from a worker node."""
        logging.info(f"[{self.address}] Requesting processed shard {shard_id} for video {video_id} from worker {worker_address}")

        worker_stub = self._worker_stubs.get(worker_address)

        
        if not worker_stub:
            logging.error(f"[{self.address}] No WorkerService stub for {worker_address}. Cannot retrieve shard {shard_id}. Marking shard as retrieval failed.")
            if shard_id in self.video_statuses[video_id]["shards"]:
                self.video_statuses[video_id]["shards"][shard_id]["status"] = "retrieval_failed"
                self.video_statuses[video_id]["shards"][shard_id]["message"] = "No worker stub available for retrieval."
            return

        try:
            request = replication_pb2.RequestShardRequest(shard_id=shard_id)
            response = await asyncio.wait_for(worker_stub.RequestShard(request), timeout=30)

            if response.success:
                logging.info(f"[{self.address}] Successfully retrieved processed shard {shard_id} from {worker_address}")
                if video_id in self.video_statuses and shard_id in self.video_statuses[video_id]["shards"]:
                    self.video_statuses[video_id]["retrieved_shards"][shard_id] = {
                        "data": response.shard_data,
                        "index": self.video_statuses[video_id]["shards"][shard_id].get("index", -1)
                    }
                    self.video_statuses[video_id]["shards"][shard_id]["status"] = "retrieved"

                    video_info = self.video_statuses[video_id]
                    total_successfully_processed_shards = sum(
                        1 for s in video_info["shards"].values() if s["status"] in ["processed_successfully", "retrieved"]
                    )
                    retrieved_count = sum(
                        1 for s in video_info["shards"].values() if s["status"] == "retrieved"
                    )

                    logging.info(f"[{self.address}] Video {video_id} — retrieved {retrieved_count}/{total_successfully_processed_shards} processed shards.")
                    video_info = self.video_statuses[video_id]
                    total_shards = video_info["total_shards"]
                    retrieved_count = len(video_info["retrieved_shards"])
                    # ✅ If all processed shards retrieved → set status + concat
                    if retrieved_count == total_shards:
                        if video_info["concatenation_task"] is None or video_info["concatenation_task"].done():
                            logging.info(f"All {total_shards} shards retrieved. Starting concatenation.")
                            video_info["status"] = "concatenating"
                            concat_task = asyncio.create_task(self._concatenate_shards(video_id))
                            self._background_tasks.append(concat_task)

                else:
                    logging.warning(f"[{self.address}] Received processed shard {shard_id} for video {video_id} but video/shard info not found in status tracking. Dropping shard data.")

            else:
                logging.error(f"[{self.address}] Worker {worker_address} failed to provide shard {shard_id}: {response.message}. Marking shard as retrieval failed.")
                if shard_id in self.video_statuses[video_id]["shards"]:
                    self.video_statuses[video_id]["shards"][shard_id]["status"] = "retrieval_failed"
                    self.video_statuses[video_id]["shards"][shard_id]["message"] = response.message

        except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
            logging.error(f"[{self.address}] RPC failed or timed out when retrieving shard {shard_id} from {worker_address}: {e.code()} - {e.details()}. Marking shard as retrieval failed.", exc_info=True)
            if shard_id in self.video_statuses[video_id]["shards"]:
                self.video_statuses[video_id]["shards"][shard_id]["status"] = "retrieval_rpc_failed"
                self.video_statuses[video_id]["shards"][shard_id]["message"] = f"RPC failed or timed out: {type(e).__name__} - {e}"
        except Exception as e:
            logging.error(f"[{self.address}] Failed to retrieve shard {shard_id} from {worker_address}: {type(e).__name__} - {e}", exc_info=True)
            if shard_id in self.video_statuses[video_id]["shards"]:
                self.video_statuses[video_id]["shards"][shard_id]["status"] = "retrieval_failed"
                self.video_statuses[video_id]["shards"][shard_id]["message"] = f"Retrieval failed: {type(e).__name__} - {e}"



    async def _concatenate_shards(self, video_id: str):
        """Concatenates all retrieved shards into the final processed video."""
        logging.info(f"[{self.address}] Starting concatenation for video {video_id}")

        if video_id not in self.video_statuses:
            logging.error(f"Cannot concatenate shards. Video ID {video_id} not found.")
            return

        video_info = self.video_statuses[video_id]
        shards = video_info["retrieved_shards"]
        container = video_info.get("container", "mp4")  # Ensure container is stored during upload

        # Sort shards by index
        sorted_shards = sorted(shards.items(), key=lambda item: item[1]["index"])

        tmp_dir = tempfile.mkdtemp(prefix=f"concat_{video_id}_")
        file_list_path = os.path.join(tmp_dir, "file_list.txt")
        output_path = os.path.join(MASTER_DATA_DIR, f"{video_id}_processed.{container}")

        try:
            # Write shard data to temp files and create file list
            with open(file_list_path, 'w') as f:
                for shard_id, shard_data in sorted_shards:
                    shard_filename = os.path.join(tmp_dir, shard_id)
                    with open(shard_filename, 'wb') as shard_file:
                        shard_file.write(shard_data["data"])
                    f.write(f"file '{shard_filename}'\n")

            # Validate all shard files exist
            for shard_id, _ in sorted_shards:
                if not os.path.exists(os.path.join(tmp_dir, shard_id)):
                    raise FileNotFoundError(f"Shard {shard_id} missing in temp dir.")

            # Build FFmpeg command with error handling
            ffmpeg_cmd = [
                "ffmpeg",
                "-y",
                "-f", "concat",
                "-safe", "0",
                "-copytb", "1",
                "-i", file_list_path,
                "-c", "copy",
                output_path
            ]

            logging.info(f"[{self.address}] Running FFmpeg: {' '.join(ffmpeg_cmd)}")
            result = subprocess.run(
                ffmpeg_cmd,
                capture_output=True,
                text=True,
                check=True
            )

            logging.info(f"[{self.address}] Concatenation succeeded: {output_path}")
            video_info["status"] = "completed"
            video_info["processed_video_path"] = output_path

        except subprocess.CalledProcessError as e:
            logging.error(f"[{self.address}] FFmpeg failed (code {e.returncode}): {e.stderr}")
            video_info["status"] = "concatenation_failed"
            video_info["message"] = f"FFmpeg error: {e.stderr}"
        except Exception as e:
            logging.error(f"[{self.address}] Concatenation error: {e}")
            video_info["status"] = "concatenation_failed"
            video_info["message"] = str(e)
        finally:
            shutil.rmtree(tmp_dir)
            logging.info(f"[{self.address}] Cleaned up temp dir: {tmp_dir}")


    # Helper functions for blocking file operations to be used with run_in_executor
    def _write_file_blocking(self, filepath, data):
        with open(filepath, 'wb') as f:
            f.write(data)

    async def RetrieveVideo(self, request: replication_pb2.RetrieveVideoRequest, context: grpc.aio.ServicerContext) -> AsyncIterator[replication_pb2.RetrieveVideoChunk]:
        """Handles video retrieval requests (master only)."""
        if self.role != 'master':
             logging.error(f"[{self.address}] RetrieveVideo request received by non-master node.")
             await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "This node is not the master.")
             return

        video_id = request.video_id
        logging.info(f"[{self.address}] Received RetrieveVideo request for video ID: {video_id}")

        video_info = self.video_statuses.get(video_id)
        if not video_info:
             logging.error(f"[{self.address}] Video not found for retrieval: {video_id}")
             await context.abort(grpc.StatusCode.NOT_FOUND, "Video not found.")
             return

        if video_info["status"] != "completed":
             status_message = f"Video processing status: {video_info['status']}. Not yet completed."
             logging.error(f"[{self.address}] Video not completed for retrieval: {video_id}. Status: {video_info['status']}")
             await context.abort(grpc.StatusCode.FAILED_PRECONDITION, status_message)
             return

        processed_video_path = video_info.get("processed_video_path")
        if not processed_video_path or not os.path.exists(processed_video_path):
             logging.error(f"[{self.address}] Processed video file not found for {video_id} at {processed_video_path}")
             await context.abort(grpc.StatusCode.INTERNAL, "Processed video file not found on master.")
             return

        loop = asyncio.get_event_loop() # Get the event loop for run_in_executor

        try:
             logging.info(f"[{self.address}] Streaming processed video file {processed_video_path} for video ID: {video_id}")
             # Use run_in_executor for blocking file read
             with open(processed_video_path, 'rb') as f:
                while True:
                    chunk = await loop.run_in_executor(None, f.read, STREAM_CHUNK_SIZE)
                    if not chunk:
                        break
                    yield replication_pb2.RetrieveVideoChunk(video_id=video_id, data_chunk=chunk)

             logging.info(f"[{self.address}] Finished streaming processed video for video ID: {video_id}")

        except Exception as e:
             logging.error(f"[{self.address}] Failed to stream processed video file for {video_id}: {type(e).__name__} - {e}", exc_info=True)
             await context.abort(grpc.StatusCode.INTERNAL, f"Failed to stream processed video file: {type(e).__name__} - {e}")

    async def GetVideoStatus(self, request: replication_pb2.VideoStatusRequest, context: grpc.aio.ServicerContext) -> replication_pb2.VideoStatusResponse:
        """Provides the processing status of a video (master only)."""
        if self.role != 'master':
             return replication_pb2.VideoStatusResponse(video_id=request.video_id, status="not_master", message="This node is not the master and does not track video status.")

        video_id = request.video_id
        logging.debug(f"[{self.address}] Received GetVideoStatus request for video ID: {video_id}")

        video_info = self.video_statuses.get(video_id)
        if not video_info:
             return replication_pb2.VideoStatusResponse(video_id=video_id, status="not_found", message="Video not found.")

        status = video_info.get("status", "unknown")
        message = video_info.get("message", "")

        if status in ["segmented", "shards_distributed", "all_shards_retrieved", "processing_failed", "concatenation_failed", "concatenation_prerequisites_failed"]:
             total_shards = video_info.get("total_shards", 0)
             processed_count = sum(1 for s in video_info["shards"].values() if s["status"] == "processed_successfully" or s["status"] == "retrieved")
             retrieved_count = sum(1 for s in video_info["shards"].values() if s["status"] == "retrieved")
             failed_count = sum(1 for s in video_info["shards"].values() if s["status"] in ["failed_processing", "rpc_failed", "failed_sending", "retrieval_failed", "retrieval_rpc_failed", "failed_distribution"])
             message = f"Status: {status}. Total shards: {total_shards}. Successfully processed/retrieved: {processed_count}. Retrieved by master: {retrieved_count}. Failed: {failed_count}. Details: {message}"

        return replication_pb2.VideoStatusResponse(video_id=video_id, status=status, message=message)

    async def ProcessShard(self, request: replication_pb2.DistributeShardRequest, context: grpc.aio.ServicerContext) -> replication_pb2.ProcessShardResponse:
        logging.info(f"[{self.address}] Received ProcessShard request. Role: {self.role}")
        if self.role != 'worker':
            return replication_pb2.ProcessShardResponse(
                shard_id=request.shard_id,
                success=False,
                message="Not a worker"
            )

        video_id       = request.video_id
        shard_id       = request.shard_id       # e.g. "videoid_shard_0000.mkv"
        shard_data     = request.shard_data
        target_w       = request.target_width
        target_h       = request.target_height

        temp_in  = os.path.join(SHARDS_DIR, f"{shard_id}_input.tmp")
        # Extract container from shard_id extension
        container = shard_id.split('.')[-1]     # "mkv", "mp4", etc.
        # Build output path using same extension
        temp_out = os.path.join(
            SHARDS_DIR,
            f"{shard_id}_processed.{container}"
        )

        # Choose codecs
        vcodec = 'libx264'     if container in ('mp4','mov','mkv') else 'libvpx-vp9'
        acodec = 'aac'         if container in ('mp4','mov','mkv') else 'libvorbis'

        loop = asyncio.get_event_loop()
        try:
            # Write the incoming shard to disk
            await loop.run_in_executor(None, self._write_file_blocking, temp_in, shard_data)

            logging.info(f"[{self.address}] Processing {shard_id}: {temp_in} → {temp_out} [{container}]")
            muxer = muxer_map.get(container, container)  # fall back to container itself


            ff_opts = {
                'vf':      f'scale={target_w}:{target_h}',
                'vcodec':  vcodec,
                'acodec':  acodec,
                'preset':  'fast',
                'format':  muxer,    # <- use the muxer name here,
                'vsync': 'passthrough'  # <-- Add this line

            }
            logging.info(f"[{self.address}] FFmpeg opts: {ff_opts}")

            # Run FFmpeg in executor
            await loop.run_in_executor(None, lambda: (
                ffmpeg
                .input(temp_in)
                .output(temp_out, **ff_opts)
                .run(capture_stdout=True, capture_stderr=True, overwrite_output=True)
            ))
            logging.info(f"[{self.address}] Shard {shard_id} processed → {temp_out}")

            # Clean up input temp
            await loop.run_in_executor(None, os.remove, temp_in)

            # Report success
            task = asyncio.create_task(
                self._report_shard_status(video_id, shard_id, "processed_successfully")
            )
            self._background_tasks.append(task)
            return replication_pb2.ProcessShardResponse(
                shard_id=shard_id,
                success=True,
                message="Processed successfully"
            )

        except ffmpeg.Error as e:
            err = e.stderr.decode()
            logging.error(f"[{self.address}] FFmpeg failed: {err}")
            task = asyncio.create_task(
                self._report_shard_status(video_id, shard_id, "failed_processing", err)
            )
            self._background_tasks.append(task)
            return replication_pb2.ProcessShardResponse(
                shard_id=shard_id,
                success=False,
                message=f"FFmpeg error: {err}"
            )

        except Exception as e:
            logging.error(f"[{self.address}] Processing exception: {e}", exc_info=True)
            task = asyncio.create_task(
                self._report_shard_status(video_id, shard_id, "failed_processing", str(e))
            )
            self._background_tasks.append(task)
        return replication_pb2.ProcessShardResponse(
            shard_id=shard_id,
            success=False,
            message=f"Error: {e}"
        )

    async def RequestShard(self, request: replication_pb2.RequestShardRequest, context: grpc.aio.ServicerContext) -> replication_pb2.RequestShardResponse:
        shard_id = request.shard_id  # e.g. “videoid_shard_0002.mkv”
            # Extract container (extension) from shard_id
        container = shard_id.split(".")[-1]  # "mkv", "mp4", etc.
        processed_fn = f"{shard_id}_processed.{container}"
        processed_path = os.path.join(SHARDS_DIR, processed_fn)

        logging.info(f"[{self.address}] RequestShard for {shard_id}, looking at {processed_fn}")

        if not os.path.exists(processed_path):
            msg = "Processed shard file not found."
            logging.error(f"[{self.address}] {msg} {processed_path}")
            return replication_pb2.RequestShardResponse(
                shard_id=shard_id, success=False, message=msg
            )

        # Read the correctly-named file
        with open(processed_path, "rb") as f:
            data = f.read()

        # Optionally clean it up
        await asyncio.get_event_loop().run_in_executor(None, os.remove, processed_path)
        logging.info(f"[{self.address}] Cleaned up processed shard file: {processed_fn}")

        return replication_pb2.RequestShardResponse(
            shard_id=shard_id,
            success=True,
            shard_data=data, 
            message="OK"
        )


    def _all_shards_processed_successfully(self, video_info):
        """True if all shards were processed successfully or retrieved."""
        statuses = [s["status"] for s in video_info["shards"].values()]
        return all(status in ["processed_successfully", "retrieved"] for status in statuses)

    def _all_shards_retrieved(self, video_info):
        """True if all shards have been retrieved."""
        statuses = [s["status"] for s in video_info["shards"].values()]
        return all(status == "retrieved" for status in statuses)

    def _any_shard_failed(self, video_info):
        """True if any shard failed processing or retrieval."""
        statuses = [s["status"] for s in video_info["shards"].values()]
        return any(status.startswith("failed") or status.endswith("retrieval_failed") for status in statuses)

    async def _report_shard_status(self, video_id: str, shard_id: str, status: str, message: str = ""):
        """Reports the processing status of a shard to the master (worker only)."""
        master_stub = self._get_or_create_master_stub()
        if not master_stub:
             logging.error(f"[{self.address}] Cannot report shard status for {shard_id}. No master MasterService stub available. Storing as unreported.")
             self._unreported_processed_shards[(video_id, shard_id)] = status
             return

        request = replication_pb2.ReportWorkerShardStatusRequest(
            video_id=video_id,
            shard_id=shard_id,
            worker_address=self.address,
            status=status
        )

        try:
             logging.info(f"[{self.address}] Attempting to report status '{status}' for shard {shard_id} of video {video_id} to master {self.current_master_address} via MasterService stub")
             response = await master_stub.ReportWorkerShardStatus(request)
             if response.success:
                logging.info(f"[{self.address}] Successfully reported status for shard {shard_id}.")
                if (video_id, shard_id) in self._unreported_processed_shards:
                    del self._unreported_processed_shards[(video_id, shard_id)]
                    logging.info(f"[{self.address}] Removed shard {shard_id} from unreported list after successful report.")
             else:
                logging.error(f"[{self.address}] Master rejected shard status report for {shard_id}: {response.message}. Storing as unreported.")
                self._unreported_processed_shards[(video_id, shard_id)] = status

        except grpc.aio.AioRpcError as e:
             logging.error(f"[{self.address}] RPC failed when reporting shard status for {shard_id} to master {self.current_master_address}: {e.code()} - {e.details()}. Storing as unreported.", exc_info=True)
             self._unreported_processed_shards[(video_id, shard_id)] = status
        except Exception as e:
             logging.error(f"[{self.address}] Failed to report shard status for {shard_id} to master {self.current_master_address}: {type(e).__name__} - {e}. Storing as unreported.", exc_info=True)
             self._unreported_processed_shards[(video_id, shard_id)] = status

    async def _attempt_report_unreported_shards(self):
        """Attempts to report processed shards that failed to report earlier (worker only)."""
        if not self._unreported_processed_shards:
            logging.debug(f"[{self.address}] No unreported processed shards to report.")
            return

        logging.info(f"[{self.address}] Attempting to report {len(self._unreported_processed_shards)} unreported processed shards to the new master {self.current_master_address}.")

        shards_to_report = list(self._unreported_processed_shards.items())

        for (video_id, shard_id), status in shards_to_report:
            # Call _report_shard_status which will use the current master stub
            await self._report_shard_status(video_id, shard_id, status)
            # _report_shard_status will remove the shard from _unreported_processed_shards if successful

    
    
    async def _check_other_nodes_health(self):
        """Periodically checks the health of other known nodes (master only)."""
        # This task is added to _background_tasks when the role is master initially or becomes master
        if self.role != 'master':
             logging.debug(f"[{self.address}] Not a master ({self.role}), skipping other nodes health check routine.")
             return

        health_check_interval = 5
        jitter_range = 2

        logging.info(f"[{self.address}] Starting other nodes health check routine.")

        while True:
             # Check if the role has changed while in the loop
             if self.role != 'master':
                 logging.info(f"[{self.address}] Role changed to {self.role}, stopping other nodes health check routine.")
                 break

             await asyncio.sleep(health_check_interval + random.uniform(0, jitter_range))

             # Iterate through a copy of known_nodes as the list might change if nodes are added/removed
             for node_addr in list(self.known_nodes):
                 if node_addr == self.address:
                     continue

                 node_stub = self._node_stubs.get(node_addr)
                 if not node_stub:
                    logging.warning(f"[{self.address}] No NodeService stub available for {node_addr}. Cannot perform health check.")
                    continue

                 try:
                    # Attempt to get stats as a health check
                    await asyncio.wait_for(
                        node_stub.GetNodeStats(replication_pb2.NodeStatsRequest()),
                        timeout=3 # seconds
                    )
                    logging.debug(f"[{self.address}] Node {node_addr} health check successful.")

                 except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
                    logging.warning(f"[{self.address}] Node {node_addr} health check failed: {e}. Node is potentially down.")

                 except Exception as e:
                    logging.error(f"[{self.address}] Unexpected error during health check for {node_addr}: {type(e).__name__} - {e}", exc_info=True)



    async def _delayed_start_election(self, delay: float):
        """Waits for a random delay before starting an election."""
        try:
            logging.info(f"[{self.address}] Starting {delay:.2f}s pre-election delay.")
            await asyncio.sleep(delay)
            logging.info(f"[{self.address}] Pre-election delay finished. Proceeding to start election.")
            # Only start election if still in follower or candidate state after the delay
            if self.state in ["follower", "candidate"]:
                # Create the election task and store it
                self._election_task = asyncio.create_task(self.start_election())
                # Add to background tasks for cancellation
                self._background_tasks.append(self._election_task)
            else:
                logging.info(f"[{self.address}] No longer in follower/candidate state after delay ({self.state}). Skipping election start.")

        except asyncio.CancelledError:
            logging.info(f"[{self.address}] Pre-election delay task cancelled.")
        except Exception as e:
            logging.error(f"[{self.address}] Error during pre-election delay: {type(e).__name__} - {e}", exc_info=True)
        finally:
             # Ensure the task reference is cleared after it's done (cancelled or finished)
             self._pre_election_delay_task = None


    async def _start_election(self):
        """Starts a new election."""

        if self.state == "leader":
            logging.warning(f"[{self.address}] Attempted to start election while already leader. This is unexpected.")
            return

        logging.info(f"[{self.address}] Starting election (Term: {self.current_term + 1}).")
        self.current_term += 1
        self.state = "candidate"
        self.voted_for = self.address  # Vote for self
        votes_received = 1  # Start with one vote (self)

        vote_requests = []
        for node_addr in self._node_stubs:
            if node_addr != self.address:
                stub = self._node_stubs[node_addr]
                vote_requests.append(self._send_request_vote(stub, node_addr))

        if vote_requests:
            vote_results = await asyncio.gather(*vote_requests, return_exceptions=True)

            for result in vote_results:
                if isinstance(result, replication_pb2.VoteResponse) and result.vote_granted:
                    votes_received += 1
                elif isinstance(result, Exception):
                    logging.error(f"[{self.address}] Error during VoteRequest: {result}")

        total_nodes = len(self.known_nodes) + 1  # Plus self
        majority = total_nodes // 2 + 1

        logging.info(f"[{self.address}] Received {votes_received} votes out of {total_nodes} for term {self.current_term}")

        if self.state == "candidate" and votes_received >= majority:
            logging.info(f"[{self.address}] Won election for term {self.current_term}. Becoming master.")
            await self._become_master()
        elif self.state == "candidate":
            logging.info(f"[{self.address}] Lost election for term {self.current_term}.")
            self.state = "follower"
            self.voted_for = None
            #  Election failed, reset master address
            self.current_master_address = None
            self.leader_address = None

    async def _become_master(self):
        """Transitions the node to the master role."""

        logging.info(f"[{self.address}] Becoming master for term {self.current_term}.")
        self.role = 'master'
        self.state = "leader"
        self.leader_address = self.address
        self.current_master_address = self.address

        # Add MasterService and WorkerService if not already added
        if not self._master_service_added:
            replication_pb2_grpc.MasterServiceServicer.__init__(self)
            replication_pb2_grpc.add_MasterServiceServicer_to_server(self, self._server)
            self._master_service_added = True
            logging.info(f"[{self.address}] MasterServiceServicer added to server.")

        if not self._worker_service_added:
            replication_pb2_grpc.WorkerServiceServicer.__init__(self)
            replication_pb2_grpc.add_WorkerServiceServicer_to_server(self, self._server)
            self._worker_service_added = True
            logging.info(f"[{self.address}] WorkerServiceServicer added to server.")

        # Initialize worker stubs
        self._worker_stubs = {}
        for node_addr in self.known_nodes:
            if node_addr != self.address:
                self._create_stubs_for_node(node_addr)

        # Start master routines
        if self._master_announcement_task is None or self._master_announcement_task.done():
            self._master_announcement_task = asyncio.create_task(self._master_election_announcement_routine())
            self._background_tasks.append(self._master_announcement_task)
        if self._other_nodes_health_check_task is None or self._other_nodes_health_check_task.done():
            self._other_nodes_health_check_task = asyncio.create_task(self._check_other_nodes_health())
            self._background_tasks.append(self._other_nodes_health_check_task)

    async def _send_request_vote(self, stub: replication_pb2_grpc.NodeServiceStub, node_address: str) -> Optional[replication_pb2.VoteResponse]:
        """Sends a RequestVote RPC to a node."""

        try:
            request = replication_pb2.VoteRequest(
                candidate_id=self.address,
                candidate_address=self.address,
                term=self.current_term
            )
            response = await asyncio.wait_for(stub.RequestVote(request), timeout=5)
            logging.debug(f"[{self.address}] Received VoteResponse from {node_address}: {response.vote_granted}")
            return response
        except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
            logging.warning(f"[{self.address}] VoteRequest to {node_address} failed: {e}")
            return None
        except Exception as e:
            logging.error(f"[{self.address}] Unexpected error sending VoteRequest to {node_address}: {e}", exc_info=True)
            return None






    async def _request_vote_from_node(self, node_stub: replication_pb2_grpc.NodeServiceStub, node_address: str) -> bool:
        """Sends a VoteRequest to a node."""
        try:
            response = await asyncio.wait_for(
                node_stub.RequestVote(
                    replication_pb2.VoteRequest(
                        candidate_id=self.id,
                        candidate_address=self.address,
                        term=self.current_term
                    ),
                    timeout=2
                ),
                timeout=2
            )
            return response.vote_granted
        except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
            logging.warning(f"[{self.address}] Vote request to {node_address} failed: {e}")
            return False
        except Exception as e:
            logging.error(f"[{self.address}] Unexpected error requesting vote from {node_address}: {e}", exc_info=True)
            return False
    async def _request_vote_from_node(self, node_stub: replication_pb2_grpc.NodeServiceStub, request: replication_pb2.VoteRequest, node_address: str) -> replication_pb2.VoteResponse:
        """Sends a VoteRequest RPC to a specific node."""
        try:
             logging.debug(f"[{self.address}] Requesting vote from {node_address} for term {request.term}")
             response = await node_stub.RequestVote(request, timeout=3)
             # If a higher term is received in the vote response, immediately revert to follower
             if response.HasField("voter_id") and response.voter_id != "unknown" and response.voter_id != self.id: # Check if response is valid and not from self
                 # Note: The VoteResponse proto doesn't currently include the voter's term.
                 # In a real Raft, you would check response.term > self.current_term here.
                 # For this simplified model, we rely on AnnounceMaster for term updates.
                 pass # No term check in vote response for now

             return response
        except grpc.aio.AioRpcError as e:
             logging.warning(f"[{self.address}] RPC failed when requesting vote from {node_address} for term {request.term}: {e.code()} - {e.details()}")
             return replication_pb2.VoteResponse(vote_granted=False, voter_id="unknown", voter_address=node_address)
        except asyncio.TimeoutError:
             logging.warning(f"[{self.address}] Vote request to {node_address} for term {request.term} timed out.")
             return replication_pb2.VoteResponse(vote_granted=False, voter_id="unknown", voter_address=node_address)
        except Exception as e:
             logging.error(f"[{self.address}] Failed to request vote from {node_address} for term {request.term}: {type(e).__name__} - {e}", exc_info=True)
             return replication_pb2.VoteResponse(vote_granted=False, voter_id="unknown", voter_address=node_address)

    async def _master_election_announcement_routine(self):
        """Periodically announces this node as the master."""

        while self.role == 'master':  # Keep announcing while master
            logging.info(f"[{self.address}] Announcing self as master (Term: {self.current_term}).")
            announcement = replication_pb2.MasterAnnouncement(
                master_address=self.address,
                node_id_of_master=self.id,
                term=self.current_term
            )

            announcement_tasks = []
            for node_addr in self._node_stubs:
                if node_addr != self.address:
                    announcement_tasks.append(self._send_master_announcement(node_addr, announcement)) # Pass node_addr

            if announcement_tasks:
                await asyncio.gather(*announcement_tasks, return_exceptions=True)

            await asyncio.sleep(5)  # Adjust interval as needed
        logging.info(f"[{self.address}] Master announcement routine stopped.")


    async def inform_network_of_new_master(self, new_master_address: str):
        """Informs all known nodes about the new master."""
        logging.info(f"[{self.address}] Executing inform_network_of_new_master. New master is: {new_master_address}. I am {self.address}. My Term: {self.current_term}.")

        if self.address == new_master_address:
             logging.info(f"[{self.address}] I am the new master. Updating my status and informing network.")
             self.role = 'master'
             self.state = 'leader'
             self.leader_address = self.address
             self.current_master_address = self.address # Master's master is itself
             # Term increment happens in start_election()

             # Dynamically add MasterServiceServicer if not already added
             if not self._master_service_added:
                 logging.info(f"[{self.address}] Adding MasterServiceServicer to server dynamically.")
                 replication_pb2_grpc.MasterServiceServicer.__init__(self)
                 replication_pb2_grpc.add_MasterServiceServicer_to_server(self, self._server)
                 self._master_service_added = True

             # Dynamically add WorkerServiceServicer if not already added (important if a non-worker becomes master)
             if not self._worker_service_added:
                  logging.info(f"[{self.address}] Adding WorkerServiceServicer to server dynamically.")
                  replication_pb2_grpc.WorkerServiceServicer.__init__(self)
                  replication_pb2_grpc.add_WorkerServiceServicer_to_server(self, self._server)
                  self._worker_service_added = True

             # Stop master health check if it was running (only workers do this)
             if self._master_health_check_task and not self._master_health_check_task.done():
                  logging.info(f"[{self.address}] Cancelling master health check task as I became master.")
                  self._master_health_check_task.cancel()
                  # Remove from background tasks list
                  if self._master_health_check_task in self._background_tasks:
                      self._background_tasks.remove(self._master_health_check_task)
                  self._master_health_check_task = None # Clear the reference


        else:
             logging.info(f"[{self.address}] Received information about new master {new_master_address}. Updating my state.")
             # Only update if the announced master has a higher term (simplified, assuming term is implicit in announcement)
             # or if we didn't know a master before, or if we were a candidate and now see a master.
             # In a real Raft, you'd compare terms explicitly. For this model, just update if different or unknown.
             if self.current_master_address != new_master_address or self.current_master_address is None or self.state == "candidate":
                 logging.info(f"[{self.address}] Updating current master address to {new_master_address}")
                 self.current_master_address = new_master_address
                 self.leader_address = new_master_address
                 self.state = 'follower'
                 self.voted_for = None # Reset vote
                 self.last_heartbeat_time = time.monotonic() # Reset heartbeat time on receiving announcement

                 # If I am a worker, update my master stub and attempt to report unreported shards
                 if self.role == 'worker':
                    logging.info(f"[{self.address}] Updating MasterService stub to point to {new_master_address} and attempting to report unreported shards.")
                    self._create_master_stubs(new_master_address)
                    logging.info(f"[{self.address}] MasterService stub for {new_master_address} updated successfully.")
                    asyncio.create_task(self._attempt_report_unreported_shards())

                 # Ensure master health check is running if we are a worker
                 if self.role == 'worker' and (self._master_health_check_task is None or self._master_health_check_task.done()):
                      logging.info(f"[{self.address}] Starting master health check routine.")
                      self._master_health_check_task = asyncio.create_task(self.check_master_health())
                      self._background_tasks.append(self._master_health_check_task)


        announcement_request = replication_pb2.MasterAnnouncement(
            master_address=new_master_address,
            node_id_of_master=self.id if self.address == new_master_address else "unknown_id", # Send own ID if I'm the master
            term=self.current_term # Include current term in announcement
        )

        logging.info(f"[{self.address}] Announcing to known nodes: {self.known_nodes}")

        # Send announcement to all other known nodes
        announcement_tasks = []
        for node_addr in self.known_nodes:
             if node_addr != self.address:
                node_stub = self._node_stubs.get(node_addr)
                if node_stub:
                    try:
                        logging.info(f"[{self.address}] Announcing new master {new_master_address} (Term: {self.current_term}) to node {node_addr}")
                        # Create a task for each announcement and store it
                        task = asyncio.create_task(self._send_master_announcement(node_stub, announcement_request, node_addr))
                        announcement_tasks.append(task)
                        self._background_tasks.append(task) # Add to background tasks for cancellation

                    except Exception as e:
                        logging.error(f"[{self.address}] Failed to create task for master announcement to {node_addr}: {type(e).__name__} - {e}", exc_info=True)
                else:
                    logging.warning(f"[{self.address}] No NodeService stub for {node_addr}. Cannot send master announcement.")

        # Optionally, wait for all announcement tasks to complete in this round
        # await asyncio.gather(*announcement_tasks, return_exceptions=True)


    async def _send_master_announcement(self, node_address: str, announcement: replication_pb2.MasterAnnouncement):
        """Sends a MasterAnnouncement RPC to a node."""
        try:
            channel = self._get_or_create_channel(node_address)
            if channel:
                stub = replication_pb2_grpc.NodeServiceStub(channel)
                await asyncio.wait_for(stub.AnnounceMaster(announcement), timeout=5)
                logging.debug(f"[{self.address}] MasterAnnouncement sent successfully to {node_address}.")
            else:
                logging.warning(f"[{self.address}] Could not create channel to {node_address} for MasterAnnouncement.")
        except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
            logging.warning(f"[{self.address}] MasterAnnouncement to {node_address} failed: {e}")
        except Exception as e:
            logging.error(f"[{self.address}] Error sending MasterAnnouncement to {node_address}: {e}", exc_info=True)

            
    async def _start_election_with_delay(self):
        """Delays the election start by a randomized timeout."""

        if self._pre_election_delay_task and not self._pre_election_delay_task.done():
            self._pre_election_delay_task.cancel()  # Cancel any existing delay
        self.election_timeout = random.uniform(10, 15)
        logging.info(f"[{self.address}] Starting pre-election delay for {self.election_timeout:.2f} seconds.")
        self._pre_election_delay_task = asyncio.create_task(self._election_delay_coro())
        self._background_tasks.append(self._pre_election_delay_task)

    async def _election_delay_coro(self):
        try:
            await asyncio.sleep(self.election_timeout)
            await self._start_election()  # Now start the election
        except asyncio.CancelledError:
            logging.info(f"[{self.address}] Pre-election delay cancelled.")

    async def check_master_health(self):
        """Periodically checks the health of the master node."""
        while self.role == 'worker' and self.current_master_address:
            try:
                # Get a NodeServiceStub for the master
                master_node_channel = self._get_or_create_channel(self.current_master_address)
                if not master_node_channel:
                    logging.warning(f"[{self.address}] Could not get channel to master at {self.current_master_address} for health check.")
                    await self._start_election_with_delay()  # Or some election trigger
                    return
                master_node_stub = replication_pb2_grpc.NodeServiceStub(master_node_channel)

                await asyncio.wait_for(
                    master_node_stub.GetNodeStats(replication_pb2.NodeStatsRequest()),
                    timeout=5  # Adjust timeout as needed
                )
                logging.debug(f"[{self.address}] Master at {self.current_master_address} is healthy.")
                self.last_heartbeat_time = time.monotonic()  # Update heartbeat
            except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
                logging.error(f"[{self.address}] Master at {self.current_master_address} is unreachable (health check failed): {e}")
                time_since_last_heartbeat = time.monotonic() - self.last_heartbeat_time
                if time_since_last_heartbeat > self.election_timeout:
                    logging.info(f"[{self.address}] Master heartbeat timeout exceeded. Starting election.")
                    await self._start_election_with_delay() # Use the delayed start
                    break # Exit the loop
            except Exception as e:
                logging.error(f"[{self.address}] Error checking master health: {e}", exc_info=True)
            await asyncio.sleep(2)  # Adjust sleep interval
        logging.info(f"[{self.address}] Master health check routine stopped.")


async def serve(host: str, port: int, role: str, master_address: Optional[str], known_nodes: List[str]):
    """Starts the gRPC server and initializes the node."""
    node_instance = Node(host, port, role, master_address, known_nodes)
    await node_instance.start()
    return node_instance # Return the node instance for shutdown


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Distributed Video Encoding Node")
    parser.add_argument("--host", type=str, default="localhost", help="Host address to bind the server to")
    parser.add_argument("--port", type=int, required=True, help="Port to bind the server to")
    parser.add_argument("--role", type=str, choices=['master', 'worker'], required=True, help="Role of the node (master or worker)")
    parser.add_argument("--master", type=str, help="Address of the initial master node (host:port). Required for workers.")
    parser.add_argument("--nodes", type=str, nargs='*', default=[], help="List of known node addresses (host:port) in the network.")

    args = parser.parse_args()

    if args.role == 'worker' and not args.master:
        print("Error: --master is required for worker nodes.")
        sys.exit(1)

    node_address_arg = f"{args.host}:{args.port}"

    if args.role == 'worker' and args.master:
        if args.master not in args.nodes and args.master != node_address_arg:
            logging.info(f"[{node_address_arg}] Adding specified master {args.master} to list of connectable nodes for NodeServices.")
            args.nodes.append(args.master)
            args.nodes = list(set(args.nodes))

    node_instance = None # Initialize node_instance to None
    try:
        # Run the serve coroutine and get the node instance
        node_instance = asyncio.run(serve(args.host, args.port,
                                        args.role, args.master, args.nodes))
    except KeyboardInterrupt:
        print(f"\n[{args.host}:{args.port}] Node interrupted by user.")
    except Exception as e:
        logging.error(f"[{args.host}:{args.port}] Node execution failed: {type(e).__name__} - {e}", exc_info=True)
    finally:
        # Ensure graceful shutdown is attempted even if an exception occurred
        if node_instance:
             logging.info(f"[{args.host}:{args.port}] Attempting graceful shutdown.")
             # Need a new event loop to run the async stop method if the main loop is closed
             try:
                 loop = asyncio.get_running_loop()
             except RuntimeError:
                 # If no running loop, create a new one
                 loop = asyncio.new_event_loop()
                 asyncio.set_event_loop(loop)

             # Run the async stop method in the event loop
             loop.run_until_complete(node_instance.stop())
             # Close the new loop if we created it
             if loop != asyncio.get_event_loop_policy().get_event_loop(): # Check if it's not the default loop
                  loop.close()

        logging.info(f"[{args.host}:{args.port}] Node process finished.")


