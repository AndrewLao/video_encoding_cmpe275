# test_worker_client.py
import asyncio
import grpc
import replication_pb2
import replication_pb2_grpc
import argparse
import logging
import time
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

async def retrieve_and_save_shard(stub: replication_pb2_grpc.VideoProcessingServiceStub, shard_location: str, output_dir: str):
    """Retrieves a shard file's content from the worker and saves it locally."""
    print(f"--- Retrieving Shard from Worker: '{shard_location}' ---")

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Determine local output filename (e.g., use the last part of the shard_location path)
    local_filename = os.path.basename(shard_location)
    local_filepath = os.path.join(output_dir, local_filename)

    request = replication_pb2.GetShardRequest(shard_location=shard_location)

    try:
        start_time = time.monotonic()
        response = await stub.GetShard(request, timeout=30.0) # Use a reasonable timeout
        end_time = time.monotonic()

        print(f"--- GetShard Response ---")
        print(f"Success: {response.success}")
        print(f"Message: {response.message}")
        print(f"RPC Time Taken: {(end_time - start_time)*1000:.2f} ms")

        if response.success:
            if response.shard_data:
                try:
                    # Save the retrieved binary data to a local file
                    with open(local_filepath, "wb") as f:
                        f.write(response.shard_data)
                    print(f"Successfully retrieved and saved shard data to '{local_filepath}' ({len(response.shard_data)} bytes).")
                    return True
                except IOError as e:
                    print(f"Error saving retrieved shard data to '{local_filepath}': {e}")
                    return False
            else:
                print("GetShard successful but no shard data received.")
                return False
        else:
            print("GetShard failed.")
            return False

    except grpc.aio.AioRpcError as e:
        print(f"--- GetShard RPC Failed ---")
        print(f"Error Details: {e.details()}")
        print(f"Status Code: {e.code()}")
        return False
    except Exception as e:
        print(f"--- Unexpected Error during GetShard ---")
        print(f"Error: {e}")
        return False


async def send_video_chunks(stub: replication_pb2_grpc.VideoProcessingServiceStub, worker_address: str, video_path: str, chunk_size: int, output_dir: str):
    """Reads a video file, chunks it, sends chunks to the worker, and retrieves processed shards."""
    video_id = os.path.basename(video_path)
    print(f"--- Processing Video '{video_id}' ({video_path}) ---")

    if not os.path.exists(video_path):
        print(f"Error: Video file not found at '{video_path}'")
        return

    try:
        file_size = os.path.getsize(video_path)
        print(f"File Size: {file_size} bytes")
        print(f"Chunk Size: {chunk_size} bytes")

        chunk_index = 0
        successful_chunks_processed = 0
        successful_shards_retrieved = 0
        total_chunks = (file_size + chunk_size - 1) // chunk_size

        start_time = time.monotonic()

        with open(video_path, "rb") as f:
            while True:
                chunk_data = f.read(chunk_size)
                if not chunk_data:
                    break # End of file

                print(f"\nSending chunk {chunk_index + 1}/{total_chunks} (Size: {len(chunk_data)} bytes)...")

                request = replication_pb2.ProcessChunkRequest(
                    video_id=video_id,
                    chunk_index=chunk_index,
                    chunk_data=chunk_data
                )

                try:
                    process_response = await stub.ProcessChunk(request, timeout=30.0)

                    if process_response.success:
                        print(f"  Chunk {chunk_index + 1} processed successfully by worker {process_response.worker_id}. Shard at: {process_response.shard_location} (Encoded Size: {process_response.encoded_size} bytes)")
                        successful_chunks_processed += 1

                        # --- Retrieve the processed shard ---
                        if process_response.shard_location:
                            retrieval_success = await retrieve_and_save_shard(stub, process_response.shard_location, output_dir)
                            if retrieval_success:
                                successful_shards_retrieved += 1
                        else:
                             print(f"  Chunk {chunk_index + 1} processed but no shard_location provided for retrieval.")

                    else:
                        print(f"  Chunk {chunk_index + 1} failed processing by worker {process_response.worker_id}. Message: {process_response.message}")

                except grpc.aio.AioRpcError as e:
                    print(f"  Chunk {chunk_index + 1} RPC failed: {e.details()} (Code: {e.code()})")
                except Exception as e:
                    print(f"  Chunk {chunk_index + 1} failed with unexpected error: {e}")

                chunk_index += 1

        end_time = time.monotonic()
        elapsed_time = end_time - start_time

        print(f"\n--- Processing Complete for Video '{video_id}' ---")
        print(f"Total Chunks Sent: {chunk_index}")
        print(f"Successful Chunks Processed (Worker): {successful_chunks_processed}")
        print(f"Successful Shards Retrieved (Client): {successful_shards_retrieved}")
        print(f"Failed Chunks: {chunk_index - successful_chunks_processed}")
        print(f"Total Time: {elapsed_time:.2f} seconds")
        if elapsed_time > 0 and chunk_index > 0:
             print(f"Average time per chunk (send + process + retrieve): {(elapsed_time / chunk_index):.4f} seconds")


    except Exception as e:
        logging.error(f"An error occurred during file processing or sending: {e}", exc_info=True)


async def main():
    parser = argparse.ArgumentParser(description='Video Processing Worker Test Client (Chunking & Retrieval)')
    parser.add_argument('--worker', type=str, required=True, help='Address of the worker to connect to (e.g., localhost:50061)')
    parser.add_argument('--video_path', type=str, required=True, help='Path to the video file to process')
    parser.add_argument('--chunk_size', type=int, default=1024 * 1024, help='Size of each chunk in bytes (default: 1MB)')
    parser.add_argument('--output_dir', type=str, default='retrieved_shards', help='Directory to save retrieved shard files')

    args = parser.parse_args()

    # Use insecure channel for simplicity
    async with grpc.aio.insecure_channel(args.worker) as channel:
        # Create a stub for the VideoProcessingService
        stub = replication_pb2_grpc.VideoProcessingServiceStub(channel)
        await send_video_chunks(stub, args.worker, args.video_path, args.chunk_size, args.output_dir)


if __name__ == '__main__':
    # Ensure grpcio and protobuf are installed: pip install grpcio protobuf
    # Ensure your .proto file is compiled: python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. replication.proto
    # Make sure worker.py is running before running this client.
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nClient interrupted.")
    except Exception as e:
        logging.error(f"Client main execution failed: {e}", exc_info=True)

