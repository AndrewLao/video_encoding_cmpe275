#!/usr/bin/env python3
# test_parallel_shards.py - Tests concurrent shard operations on the worker

import asyncio
import grpc
import sys
import os
import random
import logging
import replication_pb2
import replication_pb2_grpc

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def process_chunk(worker_address: str, video_id: str, chunk_index: int, chunk_data: bytes) -> bool:
    """Processes a chunk on the worker."""
    async with grpc.aio.insecure_channel(worker_address) as channel:
        stub = replication_pb2_grpc.VideoProcessingServiceStub(channel)
        
        request = replication_pb2.ProcessChunkRequest(
            video_id=video_id,
            chunk_index=chunk_index,
            chunk_data=chunk_data
        )
        
        logging.info(f"Sending ProcessChunk request to {worker_address} for video '{video_id}' chunk {chunk_index}")
        
        try:
            response = await stub.ProcessChunk(request)
            if response.success:
                logging.info(f"Successfully processed chunk {chunk_index} for video '{video_id}'")
                return True
            else:
                logging.error(f"Failed to process chunk {chunk_index}: {response.message}")
                return False
        except grpc.RpcError as e:
            logging.error(f"RPC error: {e.code()}: {e.details()}")
            return False

async def test_parallel_chunk_processing(worker_address: str, video_id: str, num_chunks: int = 5):
    """Tests processing multiple chunks in parallel."""
    # Generate random data for each chunk
    chunk_data = []
    for i in range(num_chunks):
        # Generate random data of varying sizes (between 50KB and 200KB)
        size = random.randint(50000, 200000)
        data = bytes([random.randint(0, 255) for _ in range(size)])
        chunk_data.append(data)
    
    # Create tasks for processing each chunk in parallel
    tasks = []
    for i in range(num_chunks):
        task = process_chunk(worker_address, video_id, i, chunk_data[i])
        tasks.append(task)
    
    # Execute all tasks concurrently
    start_time = asyncio.get_event_loop().time()
    results = await asyncio.gather(*tasks)
    end_time = asyncio.get_event_loop().time()
    
    # Log the results
    success_count = sum(1 for result in results if result)
    logging.info(f"Parallel processing completed: {success_count}/{num_chunks} chunks successful")
    logging.info(f"Total time: {end_time - start_time:.2f} seconds")
    
    return success_count == num_chunks

async def test_parallel_metadata_retrieval(worker_address: str, video_id: str, num_requests: int = 10):
    """Tests retrieving metadata in parallel multiple times."""
    async def get_metadata():
        async with grpc.aio.insecure_channel(worker_address) as channel:
            stub = replication_pb2_grpc.VideoProcessingServiceStub(channel)
            request = replication_pb2.GetVideoMetadataRequest(video_id=video_id)
            try:
                response = await stub.GetVideoMetadata(request)
                return response.success
            except grpc.RpcError:
                return False
    
    # Create multiple tasks to retrieve metadata concurrently
    tasks = [get_metadata() for _ in range(num_requests)]
    
    # Execute all tasks concurrently
    start_time = asyncio.get_event_loop().time()
    results = await asyncio.gather(*tasks)
    end_time = asyncio.get_event_loop().time()
    
    # Log the results
    success_count = sum(1 for result in results if result)
    logging.info(f"Parallel metadata retrieval completed: {success_count}/{num_requests} requests successful")
    logging.info(f"Total time: {end_time - start_time:.2f} seconds")
    
    return success_count == num_requests

async def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <worker_address> [video_id]")
        print(f"  Example: {sys.argv[0]} localhost:50061 test_parallel_video")
        return
    
    worker_address = sys.argv[1]
    video_id = sys.argv[2] if len(sys.argv) > 2 else f"test_parallel_video_{random.randint(1000, 9999)}"
    
    logging.info(f"Starting parallel tests with worker at {worker_address} for video '{video_id}'")
    
    # Test parallel chunk processing
    processing_success = await test_parallel_chunk_processing(worker_address, video_id)
    
    if processing_success:
        # If processing was successful, test parallel metadata retrieval
        await test_parallel_metadata_retrieval(worker_address, video_id)
    
    logging.info("Parallel tests completed")

if __name__ == "__main__":
    asyncio.run(main())
