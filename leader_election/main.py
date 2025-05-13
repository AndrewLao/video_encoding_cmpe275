import sys
from node import RaftNode

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python main.py <node_id> <peer1> <peer2> ...")
        sys.exit(1)

    node_id = int(sys.argv[1])
    peers = sys.argv[2:]
    node = RaftNode(node_id, peers)
    node.run()
