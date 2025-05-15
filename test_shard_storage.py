#!/usr/bin/env python3
# test_shard_storage.py - Tests the shard storage functionality of the worker

import asyncio
import grpc
import sys
import os
import logging
import replication_pb2
import replication_pb2_grpc

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def test_list_shards(worker_address, video_id=None):
    """Tests the ListShards functionality."""
    async with grpc.aio.insecure_channel(worker_address) as channel:
        stub = replication_pb2_grpc.VideoProcessingServiceStub(channel)
        
        request = replication_pb2.ListShardsRequest(video_id=video_id if video_id else "")
        logging.info(f"Sending ListShards request to {worker_address} for video_id='{video_id if video_id else 'ALL'}'")
        
        try:
            response = await stub.ListShards(request)
            logging.info(f"ListShards response: {response.message}")
            
            for video in response.videos:
                logging.info(f"Video: {video.video_id} - {len(video.chunks)} chunks")
                for chunk in video.chunks:
                    logging.info(f"  Chunk {chunk.chunk_index}: {chunk.shard_location} ({chunk.size_bytes} bytes)")
                    
            return response
        except grpc.RpcError as e:
            logging.error(f"RPC error: {e.code()}: {e.details()}")
            return None

async def test_get_video_metadata(worker_address, video_id):
    """Tests the GetVideoMetadata functionality."""
    async with grpc.aio.insecure_channel(worker_address) as channel:
        stub = replication_pb2_grpc.VideoProcessingServiceStub(channel)
        
        request = replication_pb2.GetVideoMetadataRequest(video_id=video_id)
        logging.info(f"Sending GetVideoMetadata request to {worker_address} for video_id='{video_id}'")
        
        try:
            response = await stub.GetVideoMetadata(request)
            logging.info(f"GetVideoMetadata response: {response.message}")
            
            for metadata in response.chunks_metadata:
                logging.info(f"Chunk {metadata.chunk_index}:")
                logging.info(f"  Original size: {metadata.original_size} bytes")
                logging.info(f"  Encoded size: {metadata.encoded_size} bytes")
                logging.info(f"  Compression ratio: {metadata.compression_ratio:.2f}")
                logging.info(f"  Shard location: {metadata.shard_location}")
                logging.info(f"  Timestamp: {metadata.timestamp}")
                
            return response
        except grpc.RpcError as e:
            logging.error(f"RPC error: {e.code()}: {e.details()}")
            return None

async def test_get_shard(worker_address, shard_location):
    """Tests retrieving a specific shard."""
    async with grpc.aio.insecure_channel(worker_address) as channel:
        stub = replication_pb2_grpc.VideoProcessingServiceStub(channel)
        
        request = replication_pb2.GetShardRequest(shard_location=shard_location)
        logging.info(f"Sending GetShard request to {worker_address} for shard at '{shard_location}'")
        
        try:
            response = await stub.GetShard(request)
            if response.success:
                logging.info(f"Successfully retrieved shard ({len(response.shard_data)} bytes)")
            else:
                logging.error(f"Failed to retrieve shard: {response.message}")
            return response
        except grpc.RpcError as e:
            logging.error(f"RPC error: {e.code()}: {e.details()}")
            return None

async def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <worker_address> [video_id] [shard_location]")
        print(f"  Examples:")
        print(f"  {sys.argv[0]} localhost:50061")
        print(f"  {sys.argv[0]} localhost:50061 my_video_file.mp4")
        print(f"  {sys.argv[0]} localhost:50061 my_video_file.mp4 /path/to/shard/location")
        return

    worker_address = sys.argv[1]
    video_id = sys.argv[2] if len(sys.argv) > 2 else None
    shard_location = sys.argv[3] if len(sys.argv) > 3 else None

    # Test ListShards functionality
    await test_list_shards(worker_address, video_id)
    
    # If a video_id was provided, also test GetVideoMetadata
    if video_id:
        await test_get_video_metadata(worker_address, video_id)
    
    # If a shard_location was provided, also test GetShard
    if shard_location:
        await test_get_shard(worker_address, shard_location)

if __name__ == "__main__":
    asyncio.run(main())
