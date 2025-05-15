#!/usr/bin/env python3
# test_worker.py - A simple test script for testing the worker functionality directly
import asyncio
import grpc
import replication_pb2
import replication_pb2_grpc
import logging
import argparse
import os
import random
import uuid

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def test_health_check(worker_address: str) -> bool:
    """Test the worker's health check functionality."""
    try:
        async with grpc.aio.insecure_channel(worker_address) as channel:
            stub = replication_pb2_grpc.VideoProcessingServiceStub(channel)
            
            # Send a health check request
            request = replication_pb2.HealthCheckRequest(master_id="test-master")
            response = await stub.CheckHealth(request)
            
            logging.info(f"Health check response: {response}")
            return response.is_healthy
    except Exception as e:
        logging.error(f"Health check failed: {e}")
        return False

async def test_process_chunk(worker_address: str) -> bool:
    """Test the worker's ability to process a video chunk."""
    try:
        async with grpc.aio.insecure_channel(worker_address) as channel:
            stub = replication_pb2_grpc.VideoProcessingServiceStub(channel)
            
            # Create a dummy video chunk for testing
            video_id = f"test-video-{uuid.uuid4()}"
            chunk_index = 0
            # Create a small random binary data to simulate video data
            chunk_data = bytes([random.randint(0, 255) for _ in range(1024)])  # 1KB test data
            
            # Create and send the process chunk request
            request = replication_pb2.ProcessChunkRequest(
                video_id=video_id,
                chunk_index=chunk_index,
                chunk_data=chunk_data
            )
            response = await stub.ProcessChunk(request)
            
            logging.info(f"Process chunk response: {response}")
            
            # If processing was successful, test retrieving the shard
            if response.success:
                shard_location = response.shard_location
                
                # Test retrieving the shard
                get_shard_request = replication_pb2.GetShardRequest(
                    shard_location=shard_location
                )
                get_shard_response = await stub.GetShard(get_shard_request)
                
                logging.info(f"Get shard response success: {get_shard_response.success}")
                logging.info(f"Shard data length: {len(get_shard_response.shard_data)} bytes")
                
                # Test listing shards
                list_shards_request = replication_pb2.ListShardsRequest(
                    video_id=video_id
                )
                list_shards_response = await stub.ListShards(list_shards_request)
                
                logging.info(f"List shards response: {list_shards_response}")
                
                # Test getting video metadata
                metadata_request = replication_pb2.GetVideoMetadataRequest(
                    video_id=video_id
                )
                metadata_response = await stub.GetVideoMetadata(metadata_request)
                
                logging.info(f"Get video metadata response: {metadata_response}")
                
                return True
            else:
                logging.error(f"Failed to process chunk: {response.message}")
                return False
    except Exception as e:
        logging.error(f"Process chunk test failed: {e}")
        return False

async def main():
    parser = argparse.ArgumentParser(description='Test Worker Functionality')
    parser.add_argument('--worker', type=str, default='localhost:50061', help='Worker address (host:port)')
    args = parser.parse_args()
    
    worker_address = args.worker
    
    logging.info(f"Starting tests for worker at {worker_address}")
    
    # Test 1: Health Check
    logging.info("Test 1: Health Check")
    health_check_result = await test_health_check(worker_address)
    logging.info(f"Health check test {'PASSED' if health_check_result else 'FAILED'}")
    
    # Test 2: Process Chunk & Retrieve Shard
    logging.info("Test 2: Process Chunk & Retrieve Shard")
    process_chunk_result = await test_process_chunk(worker_address)
    logging.info(f"Process chunk test {'PASSED' if process_chunk_result else 'FAILED'}")
    
    # Overall test result
    if health_check_result and process_chunk_result:
        logging.info("👍 ALL TESTS PASSED! The worker is functioning correctly.")
    else:
        logging.error("❌ SOME TESTS FAILED! Check the logs for details.")

if __name__ == "__main__":
    asyncio.run(main())
