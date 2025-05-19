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

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

SHARDS_DIR = "video_shards"
MASTER_DATA_DIR = "master_data"
MASTER_RETRIEVED_SHARDS_DIR = os.path.join(MASTER_DATA_DIR, "retrieved_shards")
muxer_map = {
    'mp4':  'mp4',
    'mkv':  'matroska',
    'webm': 'webm',
    'mov':  'mov',
}



STREAM_CHUNK_SIZE = 1024 * 1024

MAX_GRPC_MESSAGE_LENGTH = 1024 * 1024 * 1024

class Node:
    def __init__(self, host: str, port: int, role: str, master_address: Optional[str], known_nodes: List[str]):
        self.host = host
        self.port = port
        self.address = f"{host}:{port}"
        self.role = role
        self.id = str(uuid.uuid4())


        os.makedirs(str(self.port) + "-" +  SHARDS_DIR, exist_ok=True)
        os.makedirs(MASTER_DATA_DIR, exist_ok=True)
        os.makedirs(MASTER_RETRIEVED_SHARDS_DIR, exist_ok=True)

        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.leader_address: Optional[str] = None
        self.election_timeout = random.uniform(10, 15)
        self.last_heartbeat_time = time.monotonic()
        self.state = "follower"

        self._background_tasks: List[asyncio.Task] = []
        self._election_task: Optional[asyncio.Task] = None
        self._pre_election_delay_task: Optional[asyncio.Task] = None
        self._master_announcement_task: Optional[asyncio.Task] = None
        self._other_nodes_health_check_task = None
        self._master_health_check_task = None

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

        self._other_node_status: Dict[str, Dict[str, Any]] = {}
        self._node_health_timeout = 10

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
                self._other_node_status[node_addr] = {"is_up": False, "last_seen": 0, "stats": None}

        if self.role == 'worker' and self.current_master_address:
             logging.info(f"[{self.address}] Creating/Updating MasterService stubs for {self.current_master_address}")
             self._create_master_stubs(self.current_master_address)

        logging.info(f"[{self.address}] Initialized as {self.role.upper()}. Master is {self.current_master_address}. My ID: {self.id}. Current Term: {self.current_term}")

    def _get_or_create_channel(self, node_address: str) -> grpc.aio.Channel:
        if node_address not in self._channels or (self._channels.get(node_address) and self._channels[node_address]._channel.closed()):
             self._channels[node_address] = grpc.aio.insecure_channel(
                 node_address,
                 options=[
                     ('grpc.max_send_message_length', MAX_GRPC_MESSAGE_LENGTH),
                     ('grpc.max_receive_message_length', MAX_GRPC_MESSAGE_LENGTH),
                 ]
             )
        return self._channels[node_address]

    def _create_stubs_for_node(self, node_address: str):
        channel = self._get_or_create_channel(node_address)
        self._node_stubs[node_address] = replication_pb2_grpc.NodeServiceStub(channel)
        if self.role == 'master':
            self._worker_stubs[node_address] = replication_pb2_grpc.WorkerServiceStub(channel)

    def _create_master_stubs(self, master_address: str):
        master_channel_valid = self._master_channel and self._master_channel_address == master_address and not self._master_channel._channel.closed()

        if master_channel_valid:
             return

        if self._master_channel:
             asyncio.create_task(self._master_channel.close())
             self._master_channel = None
             self._master_channel_address = None

        self._master_channel = grpc.aio.insecure_channel(
            master_address,
            options=[
                ('grpc.max_send_message_length', MAX_GRPC_MESSAGE_LENGTH),
                ('grpc.max_receive_message_length', MAX_GRPC_MESSAGE_LENGTH),
            ]
        )
        self._master_channel_address = master_address
        self.master_stub = replication_pb2_grpc.MasterServiceStub(self._master_channel)

    def _get_or_create_master_stub(self) -> Optional[replication_pb2_grpc.MasterServiceStub]:
        current_master_address = self.current_master_address
        if not current_master_address:
             return None
        self._create_master_stubs(current_master_address)
        return self.master_stub
    
    async def start(self):
        server_options = [
            ('grpc.max_send_message_length', MAX_GRPC_MESSAGE_LENGTH),
            ('grpc.max_receive_message_length', MAX_GRPC_MESSAGE_LENGTH),
        ]
        self._server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10), options=server_options)

        replication_pb2_grpc.NodeServiceServicer.__init__(self) 
        self._server.add_insecure_port(self.address)
        replication_pb2_grpc.add_NodeServiceServicer_to_server(self, self._server)

        if self.role == 'worker':
             replication_pb2_grpc.WorkerServiceServicer.__init__(self)
             replication_pb2_grpc.add_WorkerServiceServicer_to_server(self, self._server)
             self._worker_service_added = True

        await self._server.start()
        logging.info(f"[{self.address}] Server started on {self.address}")


        discovered_master_address: Optional[str] = None
        highest_term_found = self.current_term

       
        if self.role == 'worker' and self.current_master_address: 
            logging.info(f"[{self.address}] Worker starting with explicit master {self.current_master_address}. Verifying...")
            master_initially_reachable = False
            try:
                master_stub_for_check = self._get_or_create_master_stub()
                if master_stub_for_check: 
                   
                    stats_request = replication_pb2.NodeStatsRequest()
                    stats_response = await asyncio.wait_for(
                        master_stub_for_check.GetNodeStats(stats_request),
                        timeout=5 
                    )
                    if stats_response and stats_response.node_address == self.current_master_address:
                        logging.info(f"[{self.address}] Explicit master {self.current_master_address} confirmed via GetNodeStats.")
                        self.last_heartbeat_time = time.monotonic() 
                        master_initially_reachable = True
                    else:
                        logging.warning(f"[{self.address}] Explicit master {self.current_master_address} responded but identity mismatch or error.")
                else:
                    logging.warning(f"[{self.address}] Could not create stub for explicit master {self.current_master_address}.")

            except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
                logging.warning(f"[{self.address}] Explicit master {self.current_master_address} unreachable during initial check: {e}")
            except Exception as e:
                logging.error(f"[{self.address}] Error checking explicit master {self.current_master_address}: {e}")

            if not master_initially_reachable:
                logging.warning(f"[{self.address}] Explicit master {self.current_master_address} not reachable at startup. Will rely on follower monitor for election.")
                self.current_master_address = None 
                self.leader_address = None


        if not self.current_master_address and self.role == 'worker': 
            discovery_tasks = []
            for node_addr in self.known_nodes:
                 if node_addr != self.address:
                     node_stub = self._node_stubs.get(node_addr)
                     if node_stub:
                         discovery_tasks.append(asyncio.create_task(self._query_node_for_master(node_stub, node_addr)))

            if discovery_tasks:
                 logging.info(f"[{self.address}] Starting discovery for an existing master among known nodes.")
                 done, pending = await asyncio.wait(discovery_tasks, timeout=5) 

                 for task in done:
                     try:
                         node_addr, is_master, term = task.result()
                         if is_master and term >= highest_term_found: 
                             highest_term_found = term
                             discovered_master_address = node_addr
                             logging.info(f"[{self.address}] Discovered potential master {node_addr} with term {term}.")
                     except Exception as e:
                         logging.error(f"[{self.address}] Error processing discovery task result: {type(e).__name__} - {e}")

                 for task in pending:
                     task.cancel()

            if discovered_master_address and highest_term_found >= self.current_term:
                 self.state = "follower"
                 self.role = 'worker' 
                 self.current_term = highest_term_found
                 self.voted_for = None 
                 self.current_master_address = discovered_master_address
                 self.leader_address = discovered_master_address 
                 self.last_heartbeat_time = time.monotonic() 
                 logging.info(f"[{self.address}] Following discovered master {self.current_master_address} at term {self.current_term}.")
                 self._create_master_stubs(self.current_master_address) 

        if self.role == 'master': 
            if not self._master_service_added:
                replication_pb2_grpc.MasterServiceServicer.__init__(self)
                replication_pb2_grpc.add_MasterServiceServicer_to_server(self, self._server)
                self._master_service_added = True
            if not self._worker_service_added: 
                replication_pb2_grpc.WorkerServiceServicer.__init__(self)
                replication_pb2_grpc.add_WorkerServiceServicer_to_server(self, self._server)
                self._worker_service_added = True

            self._worker_stubs = {} 
            self._other_node_status = {}
            for node_addr in self.known_nodes:
                 if node_addr != self.address:
                     self._create_stubs_for_node(node_addr) 
                     self._other_node_status[node_addr] = {"is_up": False, "last_seen": 0, "stats": None}


            self.state = "leader" 
            self.leader_address = self.address
            self.current_term +=1 
            logging.info(f"[{self.address}] Becoming master (leader). Term: {self.current_term}")
            self._master_announcement_task = asyncio.create_task(self._master_election_announcement_routine())
            self._background_tasks.append(self._master_announcement_task)

        elif self.role == 'worker':
            self.state = "follower" 
            if not self._worker_service_added: 
                replication_pb2_grpc.WorkerServiceServicer.__init__(self)
                replication_pb2_grpc.add_WorkerServiceServicer_to_server(self, self._server)
                self._worker_service_added = True

            if self.current_master_address:
                logging.info(f"[{self.address}] Worker starting as follower to {self.current_master_address}.")
                self._create_master_stubs(self.current_master_address)
                follower_monitor_task = asyncio.create_task(self._follower_monitor_loop())
                self._background_tasks.append(follower_monitor_task)
            else:
                logging.info(f"[{self.address}] Worker starting without a known master. Follower monitor will attempt elections.")
                self.last_heartbeat_time = time.monotonic() - (self.election_timeout + 1)
                follower_monitor_task = asyncio.create_task(self._follower_monitor_loop())
                self._background_tasks.append(follower_monitor_task)

        logging.info(f"[{self.address}] Node is now running with state: {self.state}, role: {self.role}, current_term: {self.current_term}, master: {self.current_master_address}")

        await self._server.wait_for_termination()

    async def _query_node_for_master(self, node_stub: replication_pb2_grpc.NodeServiceStub, node_address: str) -> Tuple[str, bool, int]:
        if not await self._check_node_reachable(node_address):
            return node_address, False, -1  
        
        try:
            response = await asyncio.wait_for(
                node_stub.GetCurrentMaster(replication_pb2.GetCurrentMasterRequest()),
                timeout=2
            )
            is_master = (response.master_address == node_address)
            return node_address, is_master, response.term
        except (grpc.aio.AioRpcError, asyncio.TimeoutError, Exception) as e:  
            logging.error(f"[{self.address}] Error querying node {node_address}: {str(e)}")
            return node_address, False, -1

    async def stop(self):
        for task in self._background_tasks:
             if not task.done():
                task.cancel()
        await asyncio.gather(*self._background_tasks, return_exceptions=True)

        processing_task_list = list(self.processing_tasks.values())
        for task in processing_task_list:
             if not task.done():
                task.cancel()
        await asyncio.gather(*processing_task_list, return_exceptions=True)

        if self._server:
             await self._server.stop(5)

        channel_close_tasks = []
        for address, channel in self._channels.items():
             if channel and not channel._channel.closed():
                 channel_close_tasks.append(asyncio.create_task(channel.close()))

        if self._master_channel and not self._master_channel._channel.closed():
             channel_close_tasks.append(asyncio.create_task(self._master_channel.close()))

        if channel_close_tasks:
            await asyncio.gather(*channel_close_tasks, return_exceptions=True)

    
    async def AnnounceMaster(self, request: replication_pb2.MasterAnnouncementRequest, context: grpc.aio.ServicerContext) -> replication_pb2.MasterAnnouncementReply:
        logging.debug(f"[WORKER {self.address}] Received heartbeat from master {request.master_address} (term {request.term})")
        if request.term > self.current_term:
             logging.info(f"[WORKER {self.address}] Accepting new master {request.master_address} with higher term {request.term}")
             self.current_term = request.term
             self.state = "follower"
             self.voted_for = None
             self.current_master_address = request.master_address
             self.leader_address = request.master_address
             self.last_heartbeat_time = time.monotonic()
             self.role = 'worker'

             self._create_master_stubs(request.master_address)
             asyncio.create_task(self._attempt_report_unreported_shards())
             asyncio.create_task(self._delayed_start_election())  
             if self._election_task and not self._election_task.done():
                  self._election_task.cancel()
                  self._election_task = None
             if self._pre_election_delay_task and not self._pre_election_delay_task.done():
                  self._pre_election_delay_task.cancel()
                  self._pre_election_delay_task = None
             if self._master_announcement_task and not self._master_announcement_task.done():
                  self._master_announcement_task.cancel()
                  self._master_announcement_task = None

        elif request.term == self.current_term:
             if self.state == "follower":
                 self.current_master_address = request.master_address
                 self.leader_address = request.master_address
                 self.last_heartbeat_time = time.monotonic()
                 if self.role == 'worker':
                    self._create_master_stubs(request.master_address)
                    asyncio.create_task(self._attempt_report_unreported_shards())
                 asyncio.create_task(self._delayed_start_election())  
             elif self.state == "candidate":
                 self.state = "follower"
                 self.voted_for = None
                 self.current_master_address = request.master_address
                 self.leader_address = request.leader_address
                 self.last_heartbeat_time = time.monotonic()
                 self.role = 'worker'

                 self._create_master_stubs(request.master_address)
                 asyncio.create_task(self._attempt_report_unreported_shards())

                 if self._pre_election_delay_task and not self._pre_election_delay_task.done():
                      self._pre_election_delay_task.cancel()
                      self._pre_election_delay_task = None
                 if self._election_task and not self._election_task.done():
                      self._election_task.cancel()
                      self._election_task = None
                 asyncio.create_task(self._delayed_start_election())  
        elif request.term < self.current_term:
             if self.current_master_address is not None and self.current_master_address != request.master_address:
                 pass
             else:
                 if self.current_master_address is None:
                     self.current_term = request.term
                     self.state = "follower"
                     self.voted_for = None
                     self.current_master_address = request.master_address
                     self.leader_address = request.leader_address
                     self.last_heartbeat_time = time.monotonic()
                     self.role = 'worker'

                     self._create_master_stubs(request.master_address)
                     asyncio.create_task(self._attempt_report_unreported_shards())

                     if self._pre_election_delay_task and not self._pre_election_delay_task.done():
                          self._pre_election_delay_task.cancel()
                          self._pre_election_delay_task = None

        cpu_percent = psutil.cpu_percent(interval=0.1)
        memory_info = psutil.virtual_memory()
        memory_percent = memory_info.percent
        shards_disk_usage = shutil.disk_usage(SHARDS_DIR) if os.path.exists(SHARDS_DIR) else None
        disk_space_free_shards = shards_disk_usage.free if shards_disk_usage else -1
        disk_space_total_shards = shards_disk_usage.total if shards_disk_usage else -1

        my_stats = replication_pb2.NodeStats(
            node_id=self.address,
            shard_count=len(self.processing_tasks),
            cpu_utilization=cpu_percent,
            memory_utilization=memory_percent,
            disk_space_free_shards=disk_space_free_shards,
            disk_space_total_shards=disk_space_total_shards,
        )

        response = replication_pb2.MasterAnnouncementReply(
            status=f"Acknowledged by {self.id}",
            node_id=self.id,
            follower_stats=[my_stats]
        )
        return response

    
    async def RequestVote(self, request: replication_pb2.VoteRequest, context: grpc.aio.ServicerContext) -> replication_pb2.VoteResponse:
        vote_granted = False 
        became_follower_this_call = False
        previous_master = self.current_master_address

        if request.term > self.current_term:
             logging.info(f"[{self.address}] Received VoteRequest from {request.candidate_address} with higher term {request.term}. My term {self.current_term}. Resetting state and granting vote.")
             self.current_term = request.term
             self.state = "follower" 
             self.voted_for = request.candidate_id
             vote_granted = True
             self.last_heartbeat_time = time.monotonic() 
             self.role = 'worker' 
             self.current_master_address = None 
             self.leader_address = None
             became_follower_this_call = True

             if self._election_task and not self._election_task.done(): self._election_task.cancel()
             if self._pre_election_delay_task and not self._pre_election_delay_task.done(): self._pre_election_delay_task.cancel()
             if self._master_announcement_task and not self._master_announcement_task.done(): self._master_announcement_task.cancel()


        elif request.term == self.current_term:
             if self.state == "leader":
                 logging.info(f"[{self.address}] Leader received VoteRequest from {request.candidate_address} for current term {self.current_term}. Denying vote.")
                 vote_granted = False
             elif (self.voted_for is None or self.voted_for == request.candidate_id):
                logging.info(f"[{self.address}] Received VoteRequest from {request.candidate_address} for current term {self.current_term}. Granting vote.")
                self.voted_for = request.candidate_id
                vote_granted = True
                self.last_heartbeat_time = time.monotonic() 
                if self.state == "candidate":
                    self.state = "follower"
                    self.role = 'worker'
                    self.current_master_address = None 
                    self.leader_address = None
                    became_follower_this_call = True
                    if self._pre_election_delay_task and not self._pre_election_delay_task.done(): self._pre_election_delay_task.cancel()

             else: 
                logging.info(f"[{self.address}] Received VoteRequest from {request.candidate_address} for current term {self.current_term}, but already voted for {self.voted_for}. Denying vote.")
                vote_granted = False
        else: 
             logging.info(f"[{self.address}] Received VoteRequest from {request.candidate_address} with older term {request.term}. Denying vote.")
             vote_granted = False

        if became_follower_this_call:
            if self.current_master_address and (previous_master != self.current_master_address or not self.master_stub):
                self._create_master_stubs(self.current_master_address)

            logging.info(f"[{self.address}] Became follower due to VoteRequest, ensuring follower monitor loop is active.")
            new_follower_monitor_task = asyncio.create_task(self._follower_monitor_loop())
            self._background_tasks.append(new_follower_monitor_task)


        return replication_pb2.VoteResponse(
            term=self.current_term, 
            vote_granted=vote_granted,
            voter_id=self.address, 
            voter_address=self.address
        )

    async def GetCurrentMaster(self, request: replication_pb2.GetCurrentMasterRequest, context: grpc.aio.ServicerContext) -> replication_pb2.GetCurrentMasterResponse:
        return replication_pb2.GetCurrentMasterResponse(
            master_address=self.current_master_address if self.current_master_address else "",
            term=self.current_term,
            is_master_known=self.current_master_address is not None
        )
    
    async def _follower_monitor_loop(self):
        """
        Monitors the master's heartbeats when this node is a follower.
        If heartbeats are missed beyond the election timeout, starts an election.
        """
        logging.info(f"[{self.address}] Starting follower monitor loop.")
        while self.state == "follower" and self.role == 'worker': 
            await asyncio.sleep(2) 

            if not self.current_master_address:
                if self.role == 'worker' and (not self._election_task or self._election_task.done()):
                    logging.warning(f"[{self.address}] Follower has no master, attempting to start election.")
                    self.last_heartbeat_time = time.monotonic() - (self.election_timeout + 1)


            if self.current_master_address: 
                time_since_last_heartbeat = time.monotonic() - self.last_heartbeat_time

                if time_since_last_heartbeat > self.election_timeout:
                    logging.warning(
                        f"[{self.address}] Master '{self.current_master_address}' timed out. "
                        f"Last heartbeat: {time_since_last_heartbeat:.2f}s ago. "
                        f"Election timeout: {self.election_timeout:.2f}s. Starting election."
                    )
                    if self._election_task and not self._election_task.done():
                        logging.info(f"[{self.address}] Election task already running, follower monitor yielding.")
                    elif self._pre_election_delay_task and not self._pre_election_delay_task.done():
                        logging.info(f"[{self.address}] Pre-election delay task already running, follower monitor yielding.")
                    else:
                      
                        self.current_master_address = None
                        self.leader_address = None 
                        await self._delayed_start_election(0) 
                    

            elif self.role == 'worker' and (not self._election_task or self._election_task.done()) and (not self._pre_election_delay_task or self._pre_election_delay_task.done()):
                logging.info(f"[{self.address}] Worker has no designated master and no election in progress. Attempting to start an election after a delay.")
                await self._delayed_start_election(random.uniform(1,3))


        logging.info(f"[{self.address}] Exiting follower monitor loop. Current state: {self.state}, role: {self.role}")

    async def UploadVideo(self, request_iterator: AsyncIterator[replication_pb2.UploadVideoChunk], context: grpc.aio.ServicerContext) -> replication_pb2.UploadVideoResponse:
        if self.role != 'master':
             return replication_pb2.UploadVideoResponse(success=False, message="This node is not the master.")

        video_id: Optional[str] = None
        target_width: Optional[int] = None
        target_height: Optional[Optional[int]] = None
        original_filename: Optional[str] = None
        temp_input_path: Optional[str] = None
        container: Optional[str] = None
        upscale_width: Optional[int] = None
        upscale_height: Optional[int] = None

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

            temp_input_path = os.path.join(MASTER_DATA_DIR, f"{video_id}_original.tmp")
            loop = asyncio.get_event_loop()
            with open(temp_input_path, 'wb') as f:
                await loop.run_in_executor(None, f.write, first_chunk.data_chunk)
                async for chunk_message in request_iterator:
                    if chunk_message.is_first_chunk:
                         logging.warning(f"[{self.address}] Received unexpected first chunk indicator for video ID: {video_id} in subsequent message.")
                    await loop.run_in_executor(None, f.write, chunk_message.data_chunk)

            self.video_statuses[video_id] = {
                 "status": "segmenting",
                 "container": container,
                 "target_width": target_width,
                 "target_height": target_height,
                 "original_filename": original_filename,
                 "upscale_width": upscale_width,
                 "upscale_height": upscale_height,
                 "shards": {},
                 "retrieved_shards": {},
                 "concatenation_task": None
            }

            output_pattern = os.path.join(
                MASTER_DATA_DIR,
                f"{video_id}_shard_%04d.{container}"
            )
            segment_time = 10

            try:
                await loop.run_in_executor(
                    None,
                    lambda: (
                        ffmpeg
                        .input(temp_input_path)
                        .output(
                            output_pattern,
                            format="segment",
                            segment_time=segment_time,
                            segment_format_options="fflags=+genpts",
                            reset_timestamps=1,
                            force_key_frames="expr:gte(t,n_forced*10)",
                            c="copy", 
                            map="0", 
                            write_prft=1,
                        )
                        .run(
                            capture_stdout=True,
                            capture_stderr=True,
                            overwrite_output=True
                        )
                    )
                )
                self.video_statuses[video_id]["status"] = "segmented"

                shard_files = sorted(
                    glob.glob(
                        os.path.join(MASTER_DATA_DIR, f"{video_id}_shard_*.{container}")
                    )
                )

                if not shard_files:
                    raise Exception("No video segments were created by FFmpeg.")

                self.video_statuses[video_id]["total_shards"] = len(shard_files)

                distribute_task = asyncio.create_task(
                    self._distribute_shards(
                        video_id, shard_files, target_width, target_height, original_filename, upscale_width, upscale_height, container
                    )
                )
                self._background_tasks.append(distribute_task)

                return replication_pb2.UploadVideoResponse(
                    video_id=video_id,
                    success=True,
                    message="Video uploaded and segmentation started."
                )

            except ffmpeg.Error as e:
                 self.video_statuses[video_id]["status"] = "failed_segmentation"
                 self.video_statuses[video_id]["message"] = f"FFmpeg segmentation failed: {e.stderr.decode()}"
                 return replication_pb2.UploadVideoResponse(video_id=video_id, success=False, message=f"FFmpeg segmentation failed: {e.stderr.decode()}")
            except Exception as e:
                 self.video_statuses[video_id]["status"] = "failed_segmentation"
                 self.video_statuses[video_id]["message"] = f"Segmentation failed: {type(e).__name__} - {e}"
                 return replication_pb2.UploadVideoResponse(video_id=video_id, success=False, message=f"Segmentation failed: {type(e).__name__} - {e}")

        except Exception as e:
             if temp_input_path and os.path.exists(temp_input_path):
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, os.remove, temp_input_path)
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

    async def _distribute_shards(self, video_id: str, shard_files: List[str], target_width: int, target_height: int, original_filename: str, upscale_width: int, upscale_height: int, container: str):
        for node_addr in self.known_nodes:
            if node_addr != self.address and node_addr not in self._worker_stubs:
                self._create_stubs_for_node(node_addr)
        
        available_workers = [
            addr for addr in self._worker_stubs 
            if await self._check_node_reachable(addr)
        ]
        
        if not available_workers:
            self.video_statuses[video_id]["status"] = "failed_distribution"
            self.video_statuses[video_id]["message"] = "No workers available."
            logging.error(f"[{self.address}] No workers reachable for {video_id}")
            return

        total_shards = len(shard_files)
        self.video_statuses[video_id]["total_shards"] = total_shards
        worker_index = 0
        distribution_tasks = []

        async def distribute_single_shard(shard_file: str, shard_index: int):
            nonlocal worker_index
            shard_id = os.path.basename(shard_file)
            max_retries = 3
            retry_count = 0

            while retry_count < max_retries:
                worker_address = available_workers[worker_index % len(available_workers)]
                worker_index += 1
                worker_stub = self._worker_stubs.get(worker_address)

                if not worker_stub or not await self._check_node_reachable(worker_address):
                    continue

                try:
                    shard_data = await asyncio.get_event_loop().run_in_executor(
                        None, self._read_file_blocking, shard_file
                    )

                    request = replication_pb2.DistributeShardRequest(
                        video_id=video_id,
                        shard_id=shard_id,
                        shard_data=shard_data,
                        shard_index=shard_index,
                        total_shards=total_shards,
                        target_width=target_width,
                        target_height=target_height,
                        original_filename=original_filename,
                        upscale_width=upscale_width,
                        upscale_height=upscale_height,
                        output_format=container
                    )

                    response = await asyncio.wait_for(
                        worker_stub.ProcessShard(request),
                        timeout=30000
                    )

                    if response.success:
                        logging.info(f"[{self.address}] Shard {shard_id} sent to {worker_address}")
                        self.video_statuses[video_id]["shards"][shard_id] = {
                            "status": "sent_to_worker",
                            "worker_address": worker_address,
                            "index": shard_index
                        }
                        await asyncio.get_event_loop().run_in_executor(
                            None, self._remove_file_blocking, shard_file
                        )
                        return True
                    else:
                        logging.error(f"[{self.address}] Worker {worker_address} failed to process {shard_id}")
                        retry_count += 1

                except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
                    logging.error(f"[{self.address}] Failed to send {shard_id} to {worker_address}")
                    retry_count += 1
                except Exception as e:
                    logging.error(f"[{self.address}] Error distributing {shard_id}: {str(e)}")
                    retry_count += 1

            self.video_statuses[video_id]["shards"][shard_id] = {
                "status": "failed_distribution",
                "message": f"Failed after {max_retries} attempts",
                "index": shard_index
            }
            return False

        for idx, shard_file in enumerate(shard_files):
            task = asyncio.create_task(
                distribute_single_shard(shard_file, idx)
            )
            distribution_tasks.append(task)

        results = await asyncio.gather(*distribution_tasks)

        successful = sum(results)
        if successful == total_shards:
            self.video_statuses[video_id]["status"] = "shards_distributed"
        else:
            failed = total_shards - successful
            self.video_statuses[video_id]["status"] = "partial_distribution_failed"
            self.video_statuses[video_id]["message"] = f"Failed to distribute {failed}/{total_shards} shards"

    def _read_file_blocking(self, filepath):
        with open(filepath, 'rb') as f:
            return f.read()

    def _remove_file_blocking(self, filepath):
        if os.path.exists(filepath):
            os.remove(filepath)

    async def ReportWorkerShardStatus(self, request: replication_pb2.ReportWorkerShardStatusRequest, context: grpc.aio.ServicerContext) -> replication_pb2.ReportWorkerShardStatusResponse:
        if self.role != 'master':
            return replication_pb2.ReportWorkerShardStatusResponse(success=False, message="This node is not the master.")

        video_id = request.video_id
        shard_id = request.shard_id
        worker_address = request.worker_address
        status = request.status

        if video_id not in self.video_statuses:
            return replication_pb2.ReportWorkerShardStatusResponse(success=False, message=f"Unknown video ID: {video_id}")

        if shard_id in self.video_statuses[video_id]["shards"] and self.video_statuses[video_id]["shards"][shard_id]["status"] in ["failed_sending", "rpc_failed", "failed_distribution"]:
            original_index = self.video_statuses[video_id]["shards"][shard_id].get("index", -1)
            self.video_statuses[video_id]["shards"][shard_id] = {
                "status": status,
                "worker_address": worker_address,
                "index": original_index
            }
        elif shard_id not in self.video_statuses[video_id]["shards"]:
            self.video_statuses[video_id]["shards"][shard_id] = {
                "status": status,
                "worker_address": worker_address,
                "index": -1
            }
        else:
            self.video_statuses[video_id]["shards"][shard_id]["status"] = status
            self.video_statuses[video_id]["shards"][shard_id]["worker_address"] = worker_address

        if status == "processed_successfully":
            retrieve_task = asyncio.create_task(self._retrieve_processed_shard(video_id, shard_id, worker_address))
            self._background_tasks.append(retrieve_task)

        return replication_pb2.ReportWorkerShardStatusResponse(success=True, message="Status updated.")

    async def _retrieve_processed_shard(self, video_id: str, shard_id: str, worker_address: str):
        if self.role != 'master':
            return

        worker_stub = self._worker_stubs.get(worker_address)

        if not worker_stub:
            if shard_id in self.video_statuses[video_id]["shards"]:
                self.video_statuses[video_id]["shards"][shard_id]["status"] = "retrieval_failed"
                self.video_statuses[video_id]["shards"][shard_id]["message"] = "No worker stub available for retrieval."
            return

        try:
            request = replication_pb2.RequestShardRequest(shard_id=shard_id)
            response = await asyncio.wait_for(worker_stub.RequestShard(request), timeout=30)

            if response.success:
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

                    video_info = self.video_statuses[video_id]
                    total_shards = video_info["total_shards"]
                    retrieved_count = len(video_info["retrieved_shards"])
                    if retrieved_count == total_shards:
                        if video_info["concatenation_task"] is None or video_info["concatenation_task"].done():
                            video_info["status"] = "concatenating"
                            concat_task = asyncio.create_task(self._concatenate_shards(video_id))
                            self._background_tasks.append(concat_task)

                else:
                    pass

            else:
                if shard_id in self.video_statuses[video_id]["shards"]:
                    self.video_statuses[video_id]["shards"][shard_id]["status"] = "retrieval_failed"
                    self.video_statuses[video_id]["shards"][shard_id]["message"] = response.message

        except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
            if shard_id in self.video_statuses[video_id]["shards"]:
                self.video_statuses[video_id]["shards"][shard_id]["status"] = "retrieval_rpc_failed"
                self.video_statuses[video_id]["shards"][shard_id]["message"] = f"RPC failed or timed out: {type(e).__name__} - {e}"
        except Exception as e:
            if shard_id in self.video_statuses[video_id]["shards"]:
                self.video_statuses[video_id]["shards"][shard_id]["status"] = "retrieval_failed"
                self.video_statuses[video_id]["shards"][shard_id]["message"] = f"Retrieval failed: {type(e).__name__} - {e}"

    async def _concatenate_shards(self, video_id: str):
        if video_id not in self.video_statuses:
            return

        video_info = self.video_statuses[video_id]
        shards = video_info["retrieved_shards"]
        container = video_info.get("container", "mp4")

        sorted_shards = sorted(shards.items(), key=lambda item: item[1]["index"])

        tmp_dir = tempfile.mkdtemp(prefix=f"concat_{video_id}_")
        file_list_path = os.path.join(tmp_dir, "file_list.txt")
        output_path = os.path.join(MASTER_DATA_DIR, f"{video_id}_processed.{container}")

        try:
            with open(file_list_path, 'w') as f:
                for shard_id, shard_data in sorted_shards:
                    shard_filename = os.path.join(tmp_dir, shard_id)
                    with open(shard_filename, 'wb') as shard_file:
                        shard_file.write(shard_data["data"])
                    f.write(f"file '{shard_filename}'\n")

            for shard_id, _ in sorted_shards:
                if not os.path.exists(os.path.join(tmp_dir, shard_id)):
                    raise FileNotFoundError(f"Shard {shard_id} missing in temp dir.")

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

            result = subprocess.run(
                ffmpeg_cmd,
                capture_output=True,
                text=True,
                check=True
            )

            video_info["status"] = "completed"
            video_info["processed_video_path"] = output_path

        except subprocess.CalledProcessError as e:
            video_info["status"] = "concatenation_failed"
            video_info["message"] = f"FFmpeg error: {e.stderr}"
        except Exception as e:
            video_info["status"] = "concatenation_failed"
            video_info["message"] = str(e)
        finally:
            shutil.rmtree(tmp_dir)

    def _write_file_blocking(self, filepath, data):
        with open(filepath, 'wb') as f:
            f.write(data)

    async def RetrieveVideo(self, request: replication_pb2.RetrieveVideoRequest, context: grpc.aio.ServicerContext) -> AsyncIterator[replication_pb2.RetrieveVideoChunk]:
        if self.role != 'master':
             await context.abort(grpc.StatusCode.FAILED_PRECONDITION, "This node is not the master.")
             return

        video_id = request.video_id

        video_info = self.video_statuses.get(video_id)
        if not video_info:
             await context.abort(grpc.StatusCode.NOT_FOUND, "Video not found.")
             return

        if video_info["status"] != "completed":
             status_message = f"Video processing status: {video_info['status']}. Not yet completed."
             await context.abort(grpc.StatusCode.FAILED_PRECONDITION, status_message)
             return

        processed_video_path = video_info.get("processed_video_path")
        if not processed_video_path or not os.path.exists(processed_video_path):
             await context.abort(grpc.StatusCode.INTERNAL, "Processed video file not found on master.")
             return

        loop = asyncio.get_event_loop()

        try:
             with open(processed_video_path, 'rb') as f:
                while True:
                    chunk = await loop.run_in_executor(None, f.read, STREAM_CHUNK_SIZE)
                    if not chunk:
                        break
                    yield replication_pb2.RetrieveVideoChunk(video_id=video_id, data_chunk=chunk)

        except Exception as e:
             await context.abort(grpc.StatusCode.INTERNAL, f"Failed to stream processed video file: {type(e).__name__} - {e}")

    async def GetVideoStatus(self, request: replication_pb2.VideoStatusRequest, context: grpc.aio.ServicerContext) -> replication_pb2.VideoStatusResponse:
        if self.role != 'master':
             return replication_pb2.VideoStatusResponse(video_id=request.video_id, status="not_master", message="This node is not the master and does not track video status.")

        video_id = request.video_id

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
        if self.role != 'worker':
            return replication_pb2.ProcessShardResponse(
                shard_id=request.shard_id,
                success=False,
                message="Not a worker"
            )

        video_id       = request.video_id
        shard_id       = request.shard_id
        shard_data     = request.shard_data
        target_w       = request.target_width
        target_h       = request.target_height
        upscale_w      = request.upscale_width
        upscale_h      = request.upscale_height
        container      = request.output_format

        temp_in  = os.path.join(str(self.port) + "-" + SHARDS_DIR, f"{shard_id}_input.tmp")
        temp_out = os.path.join(
           str(self.port) + "-" +  SHARDS_DIR,
            f"{shard_id}_processed.{container}"
        )

        vcodec = 'libx264' if container in ('mp4','mov','mkv') else 'libvpx-vp9'
        acodec = 'aac' if container in ('mp4','mov','mkv') else 'libvorbis'

        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(None, self._write_file_blocking, temp_in, shard_data)

            muxer = muxer_map.get(container, container)

            ff_opts = {
                'vf':      f'scale={upscale_w}:{upscale_h}', 
                'vcodec':  vcodec,
                'acodec':  acodec,
                'preset':  'fast',
                'format':  muxer,
                'vsync': 'passthrough'
            }

            await loop.run_in_executor(None, lambda: (
                ffmpeg
                .input(temp_in)
                .output(temp_out, **ff_opts)
                .run(capture_stdout=True, capture_stderr=True, overwrite_output=True)
            ))

            await loop.run_in_executor(None, os.remove, temp_in)

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
        shard_id = request.shard_id
        container = shard_id.split(".")[-1]
        processed_fn = f"{shard_id}_processed.{container}"
        processed_path = os.path.join(str(self.port) + "-" + SHARDS_DIR, processed_fn)

        if not os.path.exists(processed_path):
            msg = "Processed shard file not found."
            return replication_pb2.RequestShardResponse(
                shard_id=shard_id, success=False, message=msg
            )

        with open(processed_path, "rb") as f:
            data = f.read()

        await asyncio.get_event_loop().run_in_executor(None, os.remove, processed_path)

        return replication_pb2.RequestShardResponse(
            shard_id=shard_id,
            success=True,
            shard_data=data,
            message="OK"
        )

   
    async def _report_shard_status(self, video_id: str, shard_id: str, status: str, message: str = ""):
        master_stub = self._get_or_create_master_stub()
        if not master_stub:
             self._unreported_processed_shards[(video_id, shard_id)] = status
             return

        request = replication_pb2.ReportWorkerShardStatusRequest(
            video_id=video_id,
            shard_id=shard_id,
            worker_address=self.address,
            status=status
        )

        try:
             response = await master_stub.ReportWorkerShardStatus(request)
             if response.success:
                if (video_id, shard_id) in self._unreported_processed_shards:
                    del self._unreported_processed_shards[(video_id, shard_id)]
             else:
                self._unreported_processed_shards[(video_id, shard_id)] = status

        except grpc.aio.AioRpcError as e:
             self._unreported_processed_shards[(video_id, shard_id)] = status
        except Exception as e:
             self._unreported_processed_shards[(video_id, shard_id)] = status

    async def _attempt_report_unreported_shards(self):
        if not self._unreported_processed_shards:
            return

        shards_to_report = list(self._unreported_processed_shards.items())

        for (video_id, shard_id), status in shards_to_report:
            await self._report_shard_status(video_id, shard_id, status)


    async def _delayed_start_election(self, delay: float = 10):
        if self._pre_election_delay_task and not self._pre_election_delay_task.done():
            logging.debug(f"[{self.address}] Cancelling existing pre-election delay task.")
            self._pre_election_delay_task.cancel()
            try:
                await self._pre_election_delay_task 
            except asyncio.CancelledError:
                logging.debug(f"[{self.address}] Existing pre-election delay task cancelled successfully.")

        logging.info(f"[{self.address}] Scheduling election to start after a {delay:.2f}s delay.")
        self._pre_election_delay_task = asyncio.create_task(self._election_delay_coro(delay))
        if self._pre_election_delay_task not in self._background_tasks: 
            self._background_tasks.append(self._pre_election_delay_task)

    async def _election_delay_coro(self, actual_delay: float):
        try:
            logging.info(f"[{self.address}] Election Coroutine: will sleep for {actual_delay:.2f}s before starting election.")
            await asyncio.sleep(actual_delay) 
            logging.info(f"[{self.address}] Election Coroutine: sleep finished. Node state: {self.state}. Attempting to call start_election.")
            if self.state == "follower" or self.state == "candidate": 
                await self.start_election()
            else:
                logging.info(f"[{self.address}] Election Coroutine: State is {self.state}, not starting election.")
        except asyncio.CancelledError:
            logging.info(f"[{self.address}] Pre-election delay coro was cancelled.")
        except Exception as e:
            logging.error(f"[{self.address}] Error in pre-election delay coro: {type(e).__name__} - {e}", exc_info=True)
        finally:
            if self._pre_election_delay_task is asyncio.current_task():
                self._pre_election_delay_task = None

    async def start_election(self):
        if self.state == "leader":
            logging.warning(f"[{self.address}] start_election called while already leader. Ignoring.")
            return

        if self.state == "candidate" and self._election_task and not self._election_task.done():
            logging.info(f"[{self.address}] start_election called, but already a candidate in an ongoing election. Ignoring.")
            return

        self.current_term += 1
        self.state = "candidate"
        self.voted_for = self.address 
        self.current_master_address = None
        self.leader_address = None
        votes_received = 1

        self._election_task = asyncio.current_task()


        total_nodes_in_cluster = len(self.known_nodes) + 1 
        majority = (total_nodes_in_cluster // 2) + 1
        logging.info(f"[{self.address}] Starting election: New Term {self.current_term}. State: {self.state}. Votes received: {votes_received}/{majority} needed (Total nodes: {total_nodes_in_cluster}).")

        vote_requests_tasks = []
        other_node_addresses = [n for n in self.known_nodes if n != self.address]

        for node_addr in other_node_addresses:
            if node_addr not in self._node_stubs:
                self._create_stubs_for_node(node_addr)

            stub = self._node_stubs.get(node_addr)
            if stub:
                logging.info(f"[{self.address}] Sending VoteRequest to {node_addr} for term {self.current_term}")
                vote_requests_tasks.append(
                    asyncio.create_task(self._send_request_vote(stub, node_addr)) 
                )
            else:
                logging.warning(f"[{self.address}] Still no stub for {node_addr}. Cannot request vote.")

        if not vote_requests_tasks and not other_node_addresses:
            logging.info(f"[{self.address}] No other nodes to request votes from (either single node cluster or others unreachable).")

        if vote_requests_tasks:
            vote_results = await asyncio.gather(*vote_requests_tasks, return_exceptions=True)

            for i, result in enumerate(vote_results):
                

                if isinstance(result, replication_pb2.VoteResponse):
                    logging.info(f"[{self.address}] Received VoteResponse from {result.voter_address} (granted: {result.vote_granted}, their term: {result.term})")
                    if result.vote_granted:
                        votes_received += 1
                        logging.info(f"[{self.address}] Vote GRANTED by {result.voter_address}. Total votes: {votes_received}/{majority}")
                    else:
                        logging.warning(f"[{self.address}] Vote DENIED by {result.voter_address} (their term {result.term}, our term {self.current_term}).")

                    if result.term > self.current_term:
                        logging.info(f"[{self.address}] Stepping down: {result.voter_address} has higher term {result.term}.")
                        self.current_term = result.term
                        self.state = "follower"
                        self.voted_for = None
                        self.current_master_address = None 
                        self.leader_address = None
                        if self._election_task and not self._election_task.done(): self._election_task.cancel() 
                        return 
                elif isinstance(result, Exception):
                    logging.error(f"[{self.address}] Error in VoteRequest RPC: {type(result).__name__} - {result}")
                else:
                    logging.warning(f"[{self.address}] Unknown result type from vote request: {result}")

        logging.info(f"[{self.address}] Election voting phase ended for term {self.current_term}. Votes received: {votes_received}. Majority needed: {majority}.")

        if self.state == "candidate": 
            if votes_received >= majority:
                logging.info(f"[{self.address}] Won election for term {self.current_term}. Becoming Master.")
                await self._become_master()
            else:
                logging.info(f"[{self.address}] Lost election or not enough votes for term {self.current_term} ({votes_received}/{majority}). Reverting to follower.")
                self.state = "follower"
                self.voted_for = None
        else: 
            logging.info(f"[{self.address}] State changed to {self.state} during election for term {self.current_term}. Election outcome overridden.")

        self._election_task = None 

    async def _send_request_vote(self, stub: replication_pb2_grpc.NodeServiceStub, node_address: str) -> Optional[replication_pb2.VoteResponse]:
        try:
            request = replication_pb2.VoteRequest(
                candidate_id=self.address, 
                candidate_address=self.address,
                term=self.current_term
            )
            response = await asyncio.wait_for(stub.RequestVote(request), timeout=7)
            return response
        except grpc.aio.AioRpcError as e:
            logging.warning(f"[{self.address}] gRPC error requesting vote from {node_address}: {e.code()} - {e.details()}")
            return None 
        except asyncio.TimeoutError:
            logging.warning(f"[{self.address}] Timeout requesting vote from {node_address}.")
            return None
        except Exception as e:
            logging.error(f"[{self.address}] Unexpected error sending VoteRequest to {node_address}: {type(e).__name__} - {e}", exc_info=True)
            return None
    async def _become_master(self):

        logging.info(f"[ELECTION {self.address}] 👑 Transitioning to MASTER role in term {self.current_term}")
        self.role = 'master'
        self.state = "leader"
        self.leader_address = self.address
        self.current_master_address = self.address

        if not self._master_service_added:
            replication_pb2_grpc.MasterServiceServicer.__init__(self)
            replication_pb2_grpc.add_MasterServiceServicer_to_server(self, self._server)
            self._master_service_added = True

        if not self._worker_service_added:
            replication_pb2_grpc.WorkerServiceServicer.__init__(self)
            replication_pb2_grpc.add_WorkerServiceServicer_to_server(self, self._server)
            self._worker_service_added = True

        self._worker_stubs = {}
        self._other_node_status = {}
        for node_addr in self.known_nodes:
            if node_addr != self.address:
                self._create_stubs_for_node(node_addr)
                self._other_node_status[node_addr] = {"is_up": True, "last_seen": time.monotonic(), "stats": None}

        if self._master_announcement_task is None or self._master_announcement_task.done():
            self._master_announcement_task = asyncio.create_task(self._master_election_announcement_routine())
            self._background_tasks.append(self._master_announcement_task)

    async def _send_request_vote(self, stub: replication_pb2_grpc.NodeServiceStub, node_address: str) -> Optional[replication_pb2.VoteResponse]:
        try:
            logging.debug(f"[ELECTION {self.address}] 📨 Sending VoteRequest to {node_address} (term {self.current_term})")
            request = replication_pb2.VoteRequest(
                candidate_id=self.address,
                candidate_address=self.address,
                term=self.current_term
            )
            response = await asyncio.wait_for(stub.RequestVote(request), timeout=5)
            if response.vote_granted:
                logging.info(f"[ELECTION {self.address}] ✅ Received vote from {node_address}")
            else:
                logging.info(f"[ELECTION {self.address}] ❌ Vote denied by {node_address}")
            return response
        except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
            logging.warning(f"[ELECTION {self.address}] 🚫 No response from {node_address}: {type(e).__name__}")
            return None
        except Exception as e:
            logging.error(f"[{self.address}] Unexpected error sending VoteRequest to {node_address}: {e}", exc_info=True)
            return None
        
    async def _master_election_announcement_routine(self):
        health_check_interval = 5
        health_check_timeout = 3
        jitter_range = 1

        while self.role == 'master':
            req = replication_pb2.MasterAnnouncementRequest(
                term=self.current_term,
                master_address=self.address,
                node_id_of_master=self.id,
            )

            announcement_tasks = []
            for node_addr, stub in list(self._node_stubs.items()):
                if node_addr != self.address:
                    announcement_tasks.append(asyncio.create_task(
                        self._send_master_announcement_and_check_health(node_addr, stub, req, health_check_timeout)
                    ))

            if announcement_tasks:
                await asyncio.gather(*announcement_tasks, return_exceptions=True)

            await asyncio.sleep(health_check_interval + random.uniform(0, jitter_range))


    async def _send_master_announcement_and_check_health(self, node_address: str, stub: replication_pb2_grpc.NodeServiceStub, request: replication_pb2.MasterAnnouncementRequest, timeout: float):
        logging.debug(f"[MASTER {self.address}] Sending heartbeat to {node_address}")
        is_reachable = await self._check_node_reachable(node_address)
        if not is_reachable:
            logging.warning(f"[MASTER {self.address}] Node {node_address} is unreachable via TCP")
            self._mark_node_down(node_address)
            return

        try:
            reply: replication_pb2.MasterAnnouncementReply = await asyncio.wait_for(
                stub.AnnounceMaster(request),
                timeout=timeout,
            )
            logging.debug(f"[MASTER {self.address}] Received heartbeat ACK from {node_address}")
            self._mark_node_up(node_address, reply.follower_stats)
        except (grpc.aio.AioRpcError, asyncio.TimeoutError) as e:
            self._mark_node_down(node_address)
        except Exception as e:
            self._mark_node_down(node_address)
    def _mark_node_up(self, node_address: str, stats: List[replication_pb2.NodeStats]):
        if node_address in self._other_node_status:
            logging.info(f"[NODE {self.address}] Marking node {node_address} as UP")
            self._other_node_status[node_address]["is_up"] = True
            self._other_node_status[node_address]["last_seen"] = time.monotonic()
            self._other_node_status[node_address]["stats"] = stats
        else:
            pass

    def _mark_node_down(self, node_address: str):
        if node_address in self._other_node_status:
            logging.warning(f"[NODE {self.address}] Marking node {node_address} as DOWN")
            self._other_node_status[node_address]["is_up"] = False
            self._other_node_status[node_address]["stats"] = None
        else:
            pass

    async def _check_node_reachable(self, node_address: str) -> bool:
        logging.debug(f"[HEALTH CHECK {self.address}] Testing TCP connection to {node_address}")
        try:
            host, port = node_address.split(':')
            port = int(port)
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), 
                timeout=2
            )
            writer.close()
            await writer.wait_closed()
            logging.debug(f"[HEALTH CHECK {self.address}] Node {node_address} is reachable via TCP")
            return True
        except Exception as e:  
            logging.debug(f"[HEALTH CHECK {self.address}] Node {node_address} unreachable: {str(e)}")
            return False

    async def inform_network_of_new_master(self, new_master_address: str):
        if self.address == new_master_address:
             self.role = 'master'
             self.state = 'leader'
             self.leader_address = self.address
             self.current_master_address = self.address

             if not self._master_service_added:
                 replication_pb2_grpc.MasterServiceServicer.__init__(self)
                 replication_pb2_grpc.add_MasterServiceServicer_to_server(self, self._server)
                 self._master_service_added = True

             if not self._worker_service_added:
                  replication_pb2_grpc.WorkerServiceServicer.__init__(self)
                  replication_pb2_grpc.add_WorkerServiceServicer_to_server(self, self._server)
                  self._worker_service_added = True

             if self._master_announcement_task is None or self._master_announcement_task.done():
                 self._master_announcement_task = asyncio.create_task(self._master_election_announcement_routine())
                 self._background_tasks.append(self._master_announcement_task)

        else:
             if self.current_master_address != new_master_address or self.current_master_address is None or self.state == "candidate":
                 self.current_master_address = new_master_address
                 self.leader_address = new_master_address
                 self.state = 'follower'
                 self.voted_for = None
                 self.last_heartbeat_time = time.monotonic()
                 self.role = 'worker'

                 if self.role == 'worker':
                    self._create_master_stubs(new_master_address)
                    asyncio.create_task(self._attempt_report_unreported_shards())

        pass

async def serve(host: str, port: int, role: str, master_address: Optional[str], known_nodes: List[str]):
    node_instance = Node(host, port, role, master_address, known_nodes)
    await node_instance.start()
    return node_instance

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
            args.nodes.append(args.master)
            args.nodes = list(set(args.nodes))

    node_instance = None
    try:
        node_instance = asyncio.run(serve(args.host, args.port,
                                        args.role, args.master, args.nodes))
    except KeyboardInterrupt:
        print(f"\n[{args.host}:{args.port}] Node interrupted by user.")
    except Exception as e:
        logging.error(f"[{args.host}:{args.port}] Node execution failed: {type(e).__name__} - {e}", exc_info=True)
    finally:
        if node_instance:
             try:
                 loop = asyncio.get_running_loop()
             except RuntimeError:
                 loop = asyncio.new_event_loop()
                 asyncio.set_event_loop(loop)

             loop.run_until_complete(node_instance.stop())
             if loop != asyncio.get_event_loop_policy().get_event_loop():
                  loop.close()
