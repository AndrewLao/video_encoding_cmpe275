# worker.py
import asyncio
import grpc
import replication_pb2
import replication_pb2_grpc
import argparse
import logging
import os
import time
import random
from concurrent import futures

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Directory to store processed shards
SHARDS_DIR = "video_shards"
# Ensure the shards directory exists when the worker starts
os.makedirs(SHARDS_DIR, exist_ok=True)
logging.info(f"Ensured shards directory exists at: {os.path.abspath(SHARDS_DIR)}")

class VideoProcessingWorker(replication_pb2_grpc.VideoProcessingServiceServicer):
    """Implements the VideoProcessingService for workers."""

    def __init__(self, host: str, port: int):
        self.worker_id = f"{host}:{port}"
        self.address = f"{host}:{port}"
        logging.info(f"Initializing Video Processing Worker {self.worker_id}")
        self._file_write_lock = asyncio.Lock() # Lock for writing shards
        self._file_read_lock = asyncio.Lock() # Lock for reading shards (less critical but good practice)


    async def ProcessChunk(self, request: replication_pb2.ProcessChunkRequest, context: grpc.aio.ServicerContext) -> replication_pb2.ProcessChunkResponse:
        """Receives a video chunk, simulates encoding, and stores it as a shard."""
        video_id = request.video_id
        chunk_index = request.chunk_index
        chunk_data = request.chunk_data
        original_size = len(chunk_data)

        logging.info(f"[{self.worker_id}] Received chunk {chunk_index} for video '{video_id}' (Original size: {original_size} bytes)")

        try:
            # --- Simulate Video Encoding ---
            simulated_encoding_time = random.uniform(0.1, 1.0)
            await asyncio.sleep(simulated_encoding_time)

            reduction_factor = random.uniform(0.5, 0.8)
            encoded_data = chunk_data[:int(original_size * (1 - reduction_factor))]
            encoded_size = len(encoded_data)

            logging.info(f"[{self.worker_id}] Simulated encoding for chunk {chunk_index} (Encoded size: {encoded_size} bytes, Took {simulated_encoding_time:.2f}s)")

            # --- Simulate Sharding and Storage ---
            shard_filename = f"{video_id}_chunk_{chunk_index}.shard"
            shard_path = os.path.join(SHARDS_DIR, shard_filename)

            async with self._file_write_lock:
                 try:
                     with open(shard_path, "wb") as f:
                         f.write(encoded_data)
                     logging.info(f"[{self.worker_id}] Stored shard for chunk {chunk_index} at '{shard_path}'")
                     shard_location = os.path.abspath(shard_path)
                 except IOError as e:
                     logging.error(f"[{self.worker_id}] Failed to write shard file '{shard_path}': {e}")
                     context.set_details(f"Failed to store shard: {e}")
                     context.set_code(grpc.StatusCode.INTERNAL)
                     return replication_pb2.ProcessChunkResponse(
                         success=False,
                         worker_id=self.worker_id,
                         shard_location="",
                         original_size=original_size,
                         encoded_size=0,
                         message=f"Failed to store shard: {e}"
                     )

            logging.info(f"[{self.worker_id}] Successfully processed and stored chunk {chunk_index} for video '{video_id}'.")
            return replication_pb2.ProcessChunkResponse(
                success=True,
                worker_id=self.worker_id,
                shard_location=shard_location,
                original_size=original_size,
                encoded_size=encoded_size,
                message="Chunk processed and sharded successfully"
            )

        except Exception as e:
            logging.error(f"[{self.worker_id}] Unexpected error processing chunk {chunk_index} for video '{video_id}': {e}", exc_info=True)
            context.set_details(f"Unexpected worker error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return replication_pb2.ProcessChunkResponse(
                success=False,
                worker_id=self.worker_id,
                shard_location="",
                original_size=original_size,
                encoded_size=0,
                message=f"Unexpected worker error: {e}"
            )

    async def GetShard(self, request: replication_pb2.GetShardRequest, context: grpc.aio.ServicerContext) -> replication_pb2.GetShardResponse:
        """Retrieves the content of a stored shard file."""
        shard_location = request.shard_location
        logging.info(f"[{self.worker_id}] Received GetShard request for '{shard_location}'")

        # Basic validation: Ensure the requested path is within the SHARDS_DIR
        # This prevents clients from requesting arbitrary files on the worker's filesystem.
        # A more robust check might resolve symlinks, etc.
        abs_shard_location = os.path.abspath(shard_location)
        abs_shards_dir = os.path.abspath(SHARDS_DIR)

        if not abs_shard_location.startswith(abs_shards_dir):
            logging.warning(f"[{self.worker_id}] Attempted to access file outside SHARDS_DIR: '{shard_location}'")
            context.set_details("Access denied: Requested file is not in the shards directory.")
            context.set_code(grpc.StatusCode.PERMISSION_DENIED)
            return replication_pb2.GetShardResponse(
                success=False,
                shard_data=b"",
                message="Access denied: Requested file is not in the shards directory."
            )


        if not os.path.exists(abs_shard_location):
            logging.warning(f"[{self.worker_id}] Requested shard file not found: '{shard_location}'")
            context.set_details("Shard file not found.")
            context.set_code(grpc.StatusCode.NOT_FOUND)
            return replication_pb2.GetShardResponse(
                success=False,
                shard_data=b"",
                message="Shard file not found."
            )

        try:
            async with self._file_read_lock: # Acquire lock before reading
                 # Read the file content
                 # Use standard file I/O for simplicity
                 with open(abs_shard_location, "rb") as f:
                     shard_data = f.read()

            logging.info(f"[{self.worker_id}] Successfully read shard file '{shard_location}' ({len(shard_data)} bytes).")
            return replication_pb2.GetShardResponse(
                success=True,
                shard_data=shard_data,
                message="Shard retrieved successfully."
            )

        except IOError as e:
            logging.error(f"[{self.worker_id}] Failed to read shard file '{shard_location}': {e}", exc_info=True)
            context.set_details(f"Failed to read shard file: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return replication_pb2.GetShardResponse(
                success=False,
                shard_data=b"",
                message=f"Failed to read shard file: {e}"
            )
        except Exception as e:
            logging.error(f"[{self.worker_id}] Unexpected error retrieving shard '{shard_location}': {e}", exc_info=True)
            context.set_details(f"Unexpected worker error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            return replication_pb2.GetShardResponse(
                success=False,
                shard_data=b"",
                message=f"Unexpected worker error: {e}"
            )


async def serve(host: str, port: int):
    """Starts the gRPC server for the worker."""
    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=10))

    # Add the VideoProcessingWorker servicer to the server
    replication_pb2_grpc.add_VideoProcessingServiceServicer_to_server(VideoProcessingWorker(host, port), server)

    listen_addr = f'{host}:{port}'
    try:
        server.add_insecure_port(listen_addr)
        logging.info(f"Video Processing Worker attempting to listen on {listen_addr}")
        await server.start()
        logging.info(f"Video Processing Worker successfully listening on {listen_addr}")
    except Exception as e:
        logging.error(f"Failed to start Video Processing Worker on {listen_addr}: {e}", exc_info=True)
        raise

    try:
        await server.wait_for_termination()
    except KeyboardInterrupt:
        logging.info("Video Processing Worker interrupted by user.")
        await server.stop(grace=5)
    except Exception as e:
        logging.error(f"Video Processing Worker encountered an unexpected error during runtime: {e}", exc_info=True)
        await server.stop(grace=5)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Video Processing Worker')
    parser.add_argument('--host', type=str, default='localhost', help='Worker host to bind to (e.g., localhost, 0.0.0.0)')
    parser.add_argument('--port', type=int, required=True, help='Worker port (e.g., 50061)')

    args = parser.parse_args()

    try:
        asyncio.run(serve(args.host, args.port))
    except Exception as e:
        logging.error(f"Worker main execution failed: {e}", exc_info=True)

