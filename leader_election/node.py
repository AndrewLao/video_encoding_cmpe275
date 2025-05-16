import time, random, threading
from concurrent import futures

import grpc
import raft_pb2, raft_pb2_grpc

# Election timeout bounds (milliseconds)
ELECTION_TIMEOUT_MIN = 150
ELECTION_TIMEOUT_MAX = 300

class RaftNode(raft_pb2_grpc.RaftServicer):
    def __init__(self, node_id, peer_addresses):
        self.id = node_id
        self.peers = peer_addresses  # e.g. ["localhost:5002", "localhost:5003"]
        self.current_term = 0
        self.voted_for = None
        self.state = "follower"      # one of "follower", "candidate", "leader"
        self.lock = threading.Lock()
        self.reset_election_timer()

    def reset_election_timer(self):
        timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX) / 1000.0
        self.next_election = time.time() + timeout

    def run(self):
        # Start gRPC server
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftServicer_to_server(self, server)
        server.add_insecure_port(f"[::]:{5000 + self.id}")
        server.start()
        print(f"[Node {self.id}] Listening on port {5000 + self.id}")

        # Main loop: check for election timeout
        try:
            while True:
                time.sleep(0.01)
                with self.lock:
                    if self.state == "follower" and time.time() >= self.next_election:
                        self.start_election()
        except KeyboardInterrupt:
            server.stop(0)

    # RPC: candidate → this node
    def RequestVote(self, req, ctx):
        with self.lock:
            # If term is newer, update term and revert to follower
            if req.term > self.current_term:
                self.current_term = req.term
                self.voted_for = None
                self.state = "follower"

            vote_granted = False
            if req.term == self.current_term and (self.voted_for in (None, req.candidateId)):
                vote_granted = True
                self.voted_for = req.candidateId
                self.reset_election_timer()

            return raft_pb2.VoteResponse(term=self.current_term, voteGranted=vote_granted)

    # RPC: leader → this node (as heartbeat)
    def AppendEntries(self, req, ctx):
        with self.lock:
            if req.term >= self.current_term:
                self.current_term = req.term
                self.state = "follower"
                self.reset_election_timer()
                return raft_pb2.AppendResponse(term=self.current_term, success=True)
            else:
                return raft_pb2.AppendResponse(term=self.current_term, success=False)

    def start_election(self):
        self.state = "candidate"
        self.current_term += 1
        self.voted_for = self.id
        votes = 1
        self.reset_election_timer()
        print(f"[Node {self.id}] Starting election for term {self.current_term}")

        # Ask each peer for vote
        for addr in self.peers:
            try:
                with grpc.insecure_channel(addr) as ch:
                    stub = raft_pb2_grpc.RaftStub(ch)
                    req = raft_pb2.VoteRequest(
                        term=self.current_term,
                        candidateId=self.id,
                        lastLogIndex=0,
                        lastLogTerm=0
                    )
                    resp = stub.RequestVote(req, timeout=1)
                    if resp.voteGranted:
                        votes += 1
            except Exception:
                pass

        # If majority, become leader
        if votes > (len(self.peers) + 1) // 2:
            self.state = "leader"
            print(f"[Node {self.id}] Elected leader for term {self.current_term} ({votes}/{len(self.peers)+1} votes)")
            threading.Thread(target=self.send_heartbeats, daemon=True).start()
        else:
            print(f"[Node {self.id}] Election failed ({votes}/{len(self.peers)+1} votes), reverting to follower")
            self.state = "follower"
            # prepare for the next election
            self.reset_election_timer()

    def send_heartbeats(self):
        while True:
            with self.lock:
                if self.state != "leader":
                    break
                term, leader_id = self.current_term, self.id

            for addr in self.peers:
                try:
                    with grpc.insecure_channel(addr) as ch:
                        stub = raft_pb2_grpc.RaftStub(ch)
                        stub.AppendEntries(
                            raft_pb2.AppendRequest(term=term, leaderId=leader_id),
                            timeout=1
                        )
                except Exception:
                    pass

            time.sleep(0.05)
