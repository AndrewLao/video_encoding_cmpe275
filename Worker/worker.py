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
import psutil
import json
from concurrent import futures
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Directory structure for storing processed shards
SHARDS_DIR = "video_shards"
METADATA_DIR = os.path.join(SHARDS_DIR, "metadata")

# Ensure the shards directory structure exists when the worker starts
os.makedirs(SHARDS_DIR, exist_ok=True)
os.makedirs(METADATA_DIR, exist_ok=True)
logging.info(f"Ensured shards directory structure exists at: {os.path.abspath(SHARDS_DIR)}")
logging.info(f"Ensured metadata directory exists at: {os.path.abspath(METADATA_DIR)}")

class VideoProcessingWorker(replication_pb2_grpc.VideoProcessingServiceServicer):
    """Implements the VideoProcessingService for workers."""

    def __init__(self, host: str, port: int):
        self.worker_id = f"{host}:{port}"
        self.address = f"{host}:{port}"
        logging.info(f"Initializing Video Processing Worker {self.worker_id}")
        self._file_write_lock = asyncio.Lock() # Lock for writing shards
        self._file_read_lock = asyncio.Lock() # Lock for reading shards (less critical but good practice)
        self._active_tasks = 0  # Counter for active processing tasks
        self._active_tasks_lock = asyncio.Lock()  # Lock for the active tasks counter
        self._metadata_lock = asyncio.Lock()  # Lock for metadata operations


    async def ProcessChunk(self, request: replication_pb2.ProcessChunkRequest, context: grpc.aio.ServicerContext) -> replication_pb2.ProcessChunkResponse:
        """Receives a video chunk, simulates encoding, and stores it as a shard."""
        async with self._active_tasks_lock:
            self._active_tasks += 1
        
        try:
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

                # --- Create video-specific directory for shards ---
                video_dir = os.path.join(SHARDS_DIR, video_id)
                os.makedirs(video_dir, exist_ok=True)

                # --- Store the shard file ---
                shard_filename = f"chunk_{chunk_index}.shard"
                shard_path = os.path.join(video_dir, shard_filename)

                async with self._file_write_lock:
                     try:
                         with open(shard_path, "wb") as f:
                             f.write(encoded_data)
                         logging.info(f"[{self.worker_id}] Stored shard for chunk {chunk_index} at '{shard_path}'")
                         shard_location = os.path.abspath(shard_path)
                         
                         # --- Store metadata for the shard ---
                         metadata_result = await self._store_shard_metadata(video_id, chunk_index, original_size, encoded_size, shard_location)
                         if not metadata_result:
                             logging.error(f"[{self.worker_id}] Failed to store metadata for chunk {chunk_index}")
                             context.set_details("Failed to store shard metadata")
                             context.set_code(grpc.StatusCode.INTERNAL)
                             return replication_pb2.ProcessChunkResponse(
                                 success=False,
                                 worker_id=self.worker_id,
                                 shard_location="",
                                 original_size=original_size,
                                 encoded_size=0,
                                 message="Failed to store shard metadata"
                             )
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
        finally:
            async with self._active_tasks_lock:
                self._active_tasks -= 1

    async def _store_shard_metadata(self, video_id: str, chunk_index: int, original_size: int, encoded_size: int, shard_location: str):
        """Stores metadata for a processed shard."""
        # Create metadata with required fields
        metadata = {
            "video_id": video_id,
            "chunk_index": chunk_index,
            "original_size": original_size,
            "encoded_size": encoded_size,
            "compression_ratio": encoded_size / original_size if original_size > 0 else 0,
            "worker_id": self.worker_id,
            "shard_location": shard_location,
            "timestamp": datetime.now().isoformat()
        }
        
        # Validate metadata schema
        required_keys = ["video_id", "chunk_index", "original_size", "encoded_size", 
                         "compression_ratio", "worker_id", "shard_location", "timestamp"]
        if not all(k in metadata for k in required_keys):
            logging.error(f"[{self.worker_id}] Invalid metadata schema for chunk {chunk_index} of video '{video_id}'")
            return False
        
        metadata_filename = f"{video_id}_chunk_{chunk_index}_metadata.json"
        metadata_path = os.path.join(METADATA_DIR, metadata_filename)
        
        async with self._metadata_lock:
            try:
                with open(metadata_path, "w") as f:
                    json.dump(metadata, f, indent=2)
                logging.info(f"[{self.worker_id}] Stored metadata for chunk {chunk_index} of video '{video_id}'")
                return True
            except IOError as e:
                logging.error(f"[{self.worker_id}] Failed to write metadata file '{metadata_path}': {e}")
                return False

    async def GetShard(self, request: replication_pb2.GetShardRequest, context: grpc.aio.ServicerContext) -> replication_pb2.GetShardResponse:
        """Retrieves the content of a stored shard file."""
        shard_location = request.shard_location
        logging.info(f"[{self.worker_id}] Received GetShard request for '{shard_location}'")

        # Basic validation: Ensure the requested path is within the SHARDS_DIR
        # This prevents clients from requesting arbitrary files on the worker's filesystem.
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

    async def CheckHealth(self, request: replication_pb2.HealthCheckRequest, context: grpc.aio.ServicerContext) -> replication_pb2.HealthCheckResponse:
        """Handles health check requests from the master."""
        master_id = request.master_id
        logging.info(f"[{self.worker_id}] Received health check from master '{master_id}'")
        
        try:
            # Get system metrics using psutil
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory_usage = psutil.Process(os.getpid()).memory_info().rss  # Resident Set Size in bytes
            
            # Get number of active tasks
            active_tasks = 0
            async with self._active_tasks_lock:
                active_tasks = self._active_tasks
                
            # Create health check response
            response = replication_pb2.HealthCheckResponse(
                is_healthy=True,  # Default to healthy
                worker_id=self.worker_id,
                cpu_utilization=cpu_percent,
                memory_usage_bytes=memory_usage,
                active_tasks=active_tasks,
                message="Worker is healthy and ready to process tasks"
            )
            
            # Log health status
            logging.info(f"[{self.worker_id}] Health check completed: CPU: {cpu_percent}%, Memory: {memory_usage} bytes, Active tasks: {active_tasks}")
            
            return response
            
        except Exception as e:
            logging.error(f"[{self.worker_id}] Error during health check: {e}", exc_info=True)
            return replication_pb2.HealthCheckResponse(
                is_healthy=False,
                worker_id=self.worker_id,
                cpu_utilization=0.0,
                memory_usage_bytes=0,
                active_tasks=0,
                message=f"Health check failed: {str(e)}"
            )
    
    async def ListShards(self, request: replication_pb2.ListShardsRequest, context: grpc.aio.ServicerContext) -> replication_pb2.ListShardsResponse:
        """Lists all shards for a video or all videos stored on this worker."""
        try:
            video_id = request.video_id  # Empty string means list all videos
            logging.info(f"[{self.worker_id}] Received ListShards request for video_id='{video_id}'")
            
            if video_id:
                # List shards for a specific video
                video_dir = os.path.join(SHARDS_DIR, video_id)
                if not os.path.exists(video_dir) or not os.path.isdir(video_dir):
                    logging.warning(f"[{self.worker_id}] Video directory not found: '{video_dir}'")
                    return replication_pb2.ListShardsResponse(
                        success=False,
                        worker_id=self.worker_id,
                        message=f"Video '{video_id}' not found on this worker",
                        videos=[]
                    )
                
                # Get shard information for the specific video
                shard_files = [f for f in os.listdir(video_dir) if f.endswith('.shard')]
                chunks = []
                
                for shard_file in shard_files:
                    chunk_index = int(shard_file.split('_')[1].split('.')[0])
                    shard_path = os.path.join(video_dir, shard_file)
                    chunks.append(replication_pb2.ChunkInfo(
                        chunk_index=chunk_index,
                        shard_location=os.path.abspath(shard_path),
                        size_bytes=os.path.getsize(shard_path)
                    ))
                
                videos = [replication_pb2.VideoInfo(
                    video_id=video_id,
                    chunks=chunks
                )]
                
                return replication_pb2.ListShardsResponse(
                    success=True,
                    worker_id=self.worker_id,
                    message=f"Found {len(chunks)} shards for video '{video_id}'",
                    videos=videos
                )
            else:
                # List all videos
                videos = []
                
                # Only consider directories in SHARDS_DIR (excluding metadata)
                for dir_name in os.listdir(SHARDS_DIR):
                    dir_path = os.path.join(SHARDS_DIR, dir_name)
                    if os.path.isdir(dir_path) and dir_name != "metadata":
                        shard_files = [f for f in os.listdir(dir_path) if f.endswith('.shard')]
                        chunks = []
                        
                        for shard_file in shard_files:
                            chunk_index = int(shard_file.split('_')[1].split('.')[0])
                            shard_path = os.path.join(dir_path, shard_file)
                            chunks.append(replication_pb2.ChunkInfo(
                                chunk_index=chunk_index,
                                shard_location=os.path.abspath(shard_path),
                                size_bytes=os.path.getsize(shard_path)
                            ))
                        
                        videos.append(replication_pb2.VideoInfo(
                            video_id=dir_name,
                            chunks=chunks
                        ))
                
                return replication_pb2.ListShardsResponse(
                    success=True,
                    worker_id=self.worker_id,
                    message=f"Found {len(videos)} videos with shards",
                    videos=videos
                )
                
        except Exception as e:
            logging.error(f"[{self.worker_id}] Error in ListShards: {e}", exc_info=True)
            return replication_pb2.ListShardsResponse(
                success=False,
                worker_id=self.worker_id,
                message=f"Error listing shards: {str(e)}",
                videos=[]
            )

    async def GetVideoMetadata(self, request: replication_pb2.GetVideoMetadataRequest, context: grpc.aio.ServicerContext) -> replication_pb2.GetVideoMetadataResponse:
        """Retrieves metadata for all chunks of a specific video."""
        try:
            video_id = request.video_id
            logging.info(f"[{self.worker_id}] Received GetVideoMetadata request for video_id='{video_id}'")
            
            # Find all metadata files for this video
            metadata_files = [f for f in os.listdir(METADATA_DIR) 
                             if f.startswith(f"{video_id}_chunk_") and f.endswith("_metadata.json")]
            
            if not metadata_files:
                logging.warning(f"[{self.worker_id}] No metadata found for video '{video_id}'")
                return replication_pb2.GetVideoMetadataResponse(
                    success=False,
                    worker_id=self.worker_id,
                    message=f"No metadata found for video '{video_id}'",
                    chunks_metadata=[],
                    video_metadata=replication_pb2.VideoMetadata(
                        video_id=video_id,
                        duration="Unknown",
                        resolution="Unknown",
                        codec="Unknown",
                        total_size=0,
                        total_chunks=0,
                        creation_timestamp=datetime.now().isoformat()
                    )
                )
            
            chunks_metadata = []
            async with self._metadata_lock:
                for metadata_file in metadata_files:
                    metadata_path = os.path.join(METADATA_DIR, metadata_file)
                    try:
                        with open(metadata_path, 'r') as f:
                            metadata = json.load(f)
                            
                        chunks_metadata.append(replication_pb2.ChunkMetadata(
                            chunk_index=metadata["chunk_index"],
                            original_size=metadata["original_size"],
                            encoded_size=metadata["encoded_size"],
                            compression_ratio=metadata["compression_ratio"],
                            shard_location=metadata["shard_location"],
                            timestamp=metadata["timestamp"]
                        ))
                    except (IOError, json.JSONDecodeError) as e:
                        logging.error(f"[{self.worker_id}] Error reading metadata file '{metadata_path}': {e}")
            
            # Sort chunks by index for consistent response
            chunks_metadata.sort(key=lambda x: x.chunk_index)
            
            # Calculate total size and other aggregated metadata
            total_original_size = sum(metadata.original_size for metadata in chunks_metadata)
            total_encoded_size = sum(metadata.encoded_size for metadata in chunks_metadata)
            
            # Create aggregated video metadata
            video_metadata = replication_pb2.VideoMetadata(
                video_id=video_id,
                duration="Unknown",  # This would need an actual video parser to determine
                resolution="Unknown",  # This would need an actual video parser to determine
                codec="Unknown",      # This would need an actual video parser to determine
                total_size=total_encoded_size,
                total_chunks=len(chunks_metadata),
                creation_timestamp=chunks_metadata[0].timestamp if chunks_metadata else datetime.now().isoformat()
            )
            
            return replication_pb2.GetVideoMetadataResponse(
                success=True,
                worker_id=self.worker_id,
                message=f"Retrieved metadata for {len(chunks_metadata)} chunks of video '{video_id}'",
                chunks_metadata=chunks_metadata,
                video_metadata=video_metadata
            )
            
        except Exception as e:
            logging.error(f"[{self.worker_id}] Error in GetVideoMetadata: {e}", exc_info=True)
            return replication_pb2.GetVideoMetadataResponse(
                success=False,
                worker_id=self.worker_id,
                message=f"Error retrieving video metadata: {str(e)}",
                chunks_metadata=[],
                video_metadata=replication_pb2.VideoMetadata(
                    video_id=request.video_id,
                    duration="Unknown",
                    resolution="Unknown",
                    codec="Unknown",
                    total_size=0,
                    total_chunks=0,
                    creation_timestamp=datetime.now().isoformat()
                )
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

