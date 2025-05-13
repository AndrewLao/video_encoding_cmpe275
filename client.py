# client.py
import asyncio
import grpc
import replication_pb2
import replication_pb2_grpc
import argparse
import os
import time
from tqdm.asyncio import tqdm  # Import tqdm for the progress bar

# Define chunk size for streaming (e.g., 1MB)
CHUNK_SIZE = 1024 * 1024  # 1MB


async def upload_video(

master_address: str,
video_path: str,
target_width: int,
target_height: int,
upscale_width: int,
upscale_height: int,
output_format: str
):
    """Uploads a video file to the master node using streaming."""
    print(
        f"--- Uploading Video '{os.path.basename(video_path)}' ({video_path}) to Master ({master_address}) ---")
    print(f"Target Resolution: {target_width}x{target_height}")

    # Use filename as a simple video_id
    video_id = os.path.basename(video_path)
    original_filename = os.path.basename(video_path)

    async def generate_chunks():
        """Reads the video file in chunks and yields UploadVideoChunk messages."""
        first = True
        try:
            with open(video_path, 'rb') as f:
                while True:
                    chunk = f.read(CHUNK_SIZE)
                    if not chunk:
                        break

                    # Show you’re sending each chunk
                    print(f"chunk client size: {len(chunk)} bytes", flush=True)

                    if first:
                        yield replication_pb2.UploadVideoChunk(
                            video_id=video_id,
                            data_chunk=chunk,
                            target_width=target_width,
                            target_height=target_height,
                            upscale_width=upscale_width,
                            upscale_height=upscale_height,
                            output_format=output_format,
                            original_filename=video_id,
                            is_first_chunk=True
                        )
                        first = False
                    else:
                        # Subsequent chunks: still include video_id
                        yield replication_pb2.UploadVideoChunk(
                            video_id=video_id,
                            data_chunk=chunk,
                            is_first_chunk=False
                        )

        except FileNotFoundError:
            print(f"Error: Video file not found at {video_path}", flush=True)
        except Exception as e:
            print(f"Error reading video file: {e}", flush=True)

    # Set max message size for the channel to handle potential large chunks, though streaming
    options = [
        ('grpc.max_send_message_length', CHUNK_SIZE + 1024),
        ('grpc.max_receive_message_length', CHUNK_SIZE + 1024),
    ]
    async with grpc.aio.insecure_channel(master_address, options=options) as channel:
        stub = replication_pb2_grpc.MasterServiceStub(channel)

        print("Starting UploadVideo RPC...", flush=True)
        start = time.monotonic()
        try:
            response = await stub.UploadVideo(generate_chunks())
            elapsed = (time.monotonic() - start) * 1000
            print(f"RPC completed in {elapsed:.0f} ms", flush=True)

            print(f"→ success={response.success}, video_id={response.video_id}", flush=True)
            if response.success:
                return response.video_id
            else:
                print("Server reported failure:", response.message, flush=True)
                return None

        except grpc.aio.AioRpcError as rpc_e:
            print(f"gRPC error: {rpc_e.code()} — {rpc_e.details()}", flush=True)
            return None
        except Exception as e:
            print(f"Unexpected error during upload: {e}", flush=True)
            return None


async def retrieve_processed_video(master_address: str, video_id: str, output_path: str):
    """Retrieves a processed video from the master node using streaming with a progress bar."""
    print(
        f"\n--- Retrieving Processed Video '{video_id}' from Master ({master_address}) ---")

    channel_options = [
        # Allow chunk size + some overhead
        ('grpc.max_send_message_length', CHUNK_SIZE + 1024),
        ('grpc.max_receive_message_length', CHUNK_SIZE + 1024),
    ]
    async with grpc.aio.insecure_channel(master_address, options=channel_options) as channel:
        stub = replication_pb2_grpc.MasterServiceStub(channel)

        request = replication_pb2.RetrieveVideoRequest(video_id=video_id)

        print(f"Sending RetrieveVideo request for video ID: {video_id}...")
        start_time = time.monotonic()
        total_bytes_received = 0

        try:
            # Call the streaming RPC - this returns an async iterator
            response_stream = stub.RetrieveVideo(request)

            # Open the output file to write the received chunks
            with open(output_path, 'wb') as f:
                # Wrap the async iterator with tqdm for a progress bar
                # Since we don't know the total size beforehand, we use unit='B' and update manually
                with tqdm(unit='B', unit_scale=True, desc=f"Downloading {video_id}") as pbar:
                    async for response in response_stream:
                        # Each response contains a chunk of video data
                        chunk_size = len(response.data_chunk)
                        f.write(response.data_chunk)
                        total_bytes_received += chunk_size
                        # Update the progress bar
                        pbar.update(chunk_size)

            end_time = time.monotonic()
            rpc_time = (end_time - start_time) * 1000  # in milliseconds

            print(f"\n--- RetrieveVideo Stream Finished ---")
            print(f"Processed video saved successfully to {output_path}")
            print(f"Total bytes received: {total_bytes_received}")
            # Note: This time includes file writing
            print(f"RPC Time Taken: {rpc_time:.2f} ms")

            # Note: With server streaming, there's no single "success" field in the final response.
            # Success is implied if the stream completes without an RPC error.
            return True

        except grpc.aio.AioRpcError as e:
            print(
                f"RPC failed during retrieval stream: {e.code()} - {e.details()}")
            return False
        except Exception as e:
            print(f"An unexpected error occurred during retrieval stream: {e}")
            return False


async def get_video_status(master_address: str, video_id: str):
    """Gets the processing status of a video from the master node."""
    # print(f"\n--- Getting Video Status for '{video_id}' from Master ({master_address}) ---") # Reduced log spam

    async with grpc.aio.insecure_channel(master_address) as channel:
        stub = replication_pb2_grpc.MasterServiceStub(channel)

        request = replication_pb2.VideoStatusRequest(video_id=video_id)

        # print(f"Sending GetVideoStatus request for video ID: {video_id}...") # Reduced log spam
        start_time = time.monotonic()
        try:
            response = await stub.GetVideoStatus(request)
            end_time = time.monotonic()
            rpc_time = (end_time - start_time) * 1000  # in milliseconds

            # print("--- VideoStatus Response ---") # Reduced log spam
            # print(f"Video ID: {response.video_id}")
            print(f"[{time.strftime('%H:%M:%S')}] Video '{response.video_id}' status: {response.status}. Message: {response.message}")
            # print(f"RPC Time Taken: {rpc_time:.2f} ms") # Reduced log spam

            return response.status

        except grpc.aio.AioRpcError as e:
            print(
                f"[{time.strftime('%H:%M:%S')}] RPC failed during status check for {video_id}: {e.code()} - {e.details()}")
            return "rpc_failed"
        except Exception as e:
            print(
                f"[{time.strftime('%H:%M:%S')}] An unexpected error occurred during status check for {video_id}: {e}")
            return "error"


async def main():
    parser = argparse.ArgumentParser(
        description="Distributed Video Encoding Client")
    parser.add_argument("--master", type=str, required=True,
                        help="Address of the master node (host:port)")
    parser.add_argument("--upload", type=str,
                        help="Path to the video file to upload")
    parser.add_argument("--retrieve", type=str, help="Video ID to retrieve")
    parser.add_argument("--output", type=str,
                        help="Output path to save the retrieved video")
    parser.add_argument("--width", type=int, default=640,
                        help="Target width for video encoding")
    parser.add_argument("--height", type=int, default=480,
                        help="Target height for video encoding")
    parser.add_argument("--upscale-width",  type=int,
                        help="Upscale width for output shards")
    parser.add_argument("--upscale-height", type=int,
                        help="Upscale height for output shards")
    parser.add_argument("--format", dest="format", default="mp4",
                        help="Output container format for shards (mp4,mkv,webm,...)")
    parser.add_argument("--status", type=str,
                        help="Video ID to get status for")
    parser.add_argument("--poll-interval", type=int, default=5,
                        help="Interval in seconds to poll for video status after upload")
    parser.add_argument("--poll-timeout", type=int, default=600,
                        help="Maximum time in seconds to poll for video status after upload (default 10 mins)")

    args = parser.parse_args()

    if args.upload:
        # Pass new upscale and format args (fallback to target dims if not supplied)
        uw = args.upscale_width  if args.upscale_width  else args.width
        uh = args.upscale_height if args.upscale_height else args.height
        video_id = await upload_video(
            args.master,
            args.upload,
            args.width,
            args.height,
            uw,
            uh,
            args.format
        )
        if video_id:
            # Changed message
            print(f"\nVideo upload initiated with ID: {video_id}")

            if args.output:
                print(f"Polling master for status of video '{video_id}'...")
                start_polling_time = time.monotonic()

                while True:
                    current_status = await get_video_status(video_id=video_id, master_address=args.master)

                    if current_status == "completed":
                        print(f"\nVideo '{video_id}' processing completed.")
                        await retrieve_processed_video(video_id=video_id, master_address=args.master, output_path=args.output)
                        break  # Exit polling loop

                    if current_status in ["failed_segmentation", "failed_distribution", "processing_failed", "concatenation_failed", "concatenation_prerequisites_failed", "not_found", "rpc_failed", "error"]:
                        print(
                            f"\nVideo '{video_id}' processing failed with status: {current_status}. Aborting retrieval.")
                        break  # Exit polling loop on failure

                    if time.monotonic() - start_polling_time > args.poll_timeout:
                        print(
                            f"\nPolling for video '{video_id}' status timed out after {args.poll_timeout} seconds. Aborting retrieval.")
                        break  # Exit polling loop on timeout

                    # Wait before polling again
                    await asyncio.sleep(args.poll_interval)

            else:
                print(
                    "Specify --output path to automatically retrieve the processed video after upload.")

    elif args.retrieve:
        if not args.output:
            print("Error: --output path is required when using --retrieve.")
            return
        await retrieve_processed_video(video_id=args.retrieve, master_address=args.master, output_path=args.output)

    elif args.status:
        await get_video_status(video_id=args.status, master_address=args.master)

    else:
        print("Please specify either --upload, --retrieve, or --status.")


if __name__ == '__main__':
    # Ensure grpcio and protobuf are installed: pip install grpcio protobuf
    # Ensure your .proto file is compiled: python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. replication.proto
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nClient interrupted by user.")
    except Exception as e:
        # Keep client error logging simple
        print(f"Client main execution failed: {e}")
