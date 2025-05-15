# test_worker_client.py
import asyncio
import grpc
import replication_pb2
import replication_pb2_grpc
import argparse
import logging
import time
import os
import tempfile # Import tempfile for temporary files
import ffmpeg # Import the ffmpeg-python library
import glob # To find saved shard files
from typing import List # Import List for type hinting
import subprocess # Import the standard subprocess module for blocking calls
import shutil # Import shutil for cleaning up temporary directories


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# IMPORTANT: Ensure your replication.proto file includes target_width and target_height
# in the ProcessChunkRequest message, AND that you have re-compiled the proto:
# python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. replication.proto


async def segment_video_by_keyframes(video_path: str, output_dir: str) -> List[str]:
    """
    Segments a video file into smaller files based on keyframes using FFmpeg.

    Args:
        video_path: Path to the input video file.
        output_dir: Directory to save the segment files.

    Returns:
        A list of paths to the generated segment files, sorted by index.
    """
    print(f"\n--- Segmenting video '{video_path}' by keyframes ---")
    os.makedirs(output_dir, exist_ok=True)

    # FFmpeg command to segment video by keyframes
    # -codec copy: Avoids re-encoding, just splits the existing streams. This is fast.
    # -map 0: Map all streams (video, audio, subtitles, etc.)
    # -f segment: Use the segment muxer
    # -segment_format mpegts: Output each segment as an MPEG-TS file (.ts)
    # -segment_time 10: Optional: Split approximately every 10 seconds (FFmpeg will still try to split at keyframes near this time)
    # -segment_frames 1: Optional: Split after every keyframe (more precise keyframe splitting)
    # -reset_timestamps 1: Reset timestamps for each segment (important for concatenation)
    # output_dir/%d.ts: Output file pattern (%d is segment index)
    # We'll use segment_frames 1 to split at every keyframe.
    output_pattern = os.path.join(output_dir, '%d.ts')

    ffmpeg_command = [
        'ffmpeg',
        '-i', video_path,
        '-codec', 'copy',         # Copy streams without re-encoding
        '-map', '0',              # Map all streams
        '-f', 'segment',          # Use the segment muxer
        '-segment_format', 'mpegts', # Output format for segments
        '-segment_frames', '1',   # Split after every keyframe
        '-reset_timestamps', '1', # Reset timestamps
        '-y',                     # Overwrite output files without asking
        output_pattern
    ]

    logging.info(f"Running FFmpeg segmentation command: {' '.join(ffmpeg_command)}")

    segment_files = []
    try:
        # Execute the command using the standard subprocess.run within asyncio.to_thread
        process = await asyncio.to_thread(
            subprocess.run,
            ffmpeg_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False # Don't raise exception for non-zero exit code immediately
        )

        if process.returncode != 0:
            error_message = process.stderr.decode('utf-8', errors='ignore')
            logging.error(f"FFmpeg segmentation failed: Return code {process.returncode}\n{error_message}")
            print(f"--- Segmentation Failed ---")
            print(f"Error: FFmpeg segmentation failed. Check logs for details.")
            return [] # Return empty list on failure
        else:
            logging.info(f"FFmpeg segmentation successful.")
            print(f"--- Segmentation Successful ---")
            # Find the generated segment files
            # Use glob to find files matching the pattern in the output directory
            segment_files = sorted(glob.glob(os.path.join(output_dir, '*.ts')),
                                   key=lambda x: int(os.path.basename(x).split('.')[0])) # Sort by index

            print(f"Generated {len(segment_files)} segments.")
            return segment_files

    except FileNotFoundError:
         logging.error(f"FFmpeg command not found. Is FFmpeg installed and in the system PATH?")
         print(f"--- Segmentation Failed ---")
         print("Error: FFmpeg command not found.")
         return []
    except Exception as e:
        logging.error(f"Unexpected error during FFmpeg segmentation: {e}", exc_info=True)
        print(f"--- Segmentation Failed ---")
        print(f"Error: Unexpected error during segmentation: {e}")
        return []


async def retrieve_and_save_shard(stub: replication_pb2_grpc.VideoProcessingServiceStub, shard_location: str, output_dir: str):
    """Retrieves a shard file's content from the worker and saves it locally."""
    print(f"--- Retrieving Shard from Worker: '{shard_location}' ---")

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Determine local output filename (e.g., use the last part of the shard_location path)
    # Ensure we keep the .ts extension added by the worker
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
                    logging.info(f"Successfully retrieved and saved shard data to '{local_filepath}' ({len(response.shard_data)} bytes).")
                    return local_filepath # Return the local path if successful
                except IOError as e:
                    logging.error(f"Error saving retrieved shard data to '{local_filepath}': {e}")
                    return None
            else:
                logging.warning("GetShard successful but no shard data received.")
                return None
        else:
            logging.error(f"GetShard failed: {response.message}")
            return None

    except grpc.aio.AioRpcError as e:
        logging.error(f"--- GetShard RPC Failed ---")
        logging.error(f"Error Details: {e.details()}")
        logging.error(f"Status Code: {e.code()}")
        return None
    except Exception as e:
        logging.error(f"--- Unexpected Error during GetShard ---")
        logging.error(f"Error: {e}")
        return None

async def concatenate_shards(shard_files: List[str], output_filename: str):
    """Concatenates a list of FFmpeg .ts shard files into a single video file."""
    if not shard_files:
        print("No shard files provided for concatenation.")
        return False

    print(f"\n--- Concatenating {len(shard_files)} shards into '{output_filename}' ---")

    # FFmpeg concat demuxer requires a file list
    # We need to sort the files by chunk index to ensure correct order
    # Assuming filenames are like video_id_chunk_X.shard.ts
    def sort_key(filepath):
        try:
            filename = os.path.basename(filepath)
            # The segment files from client-side segmentation will be like 0.ts, 1.ts, etc.
            # The worker names them video_id_chunk_X.shard.ts, where X is the segment index.
            # We need to parse the chunk index from the worker-generated filename.
            parts = filename.split('_chunk_')
            if len(parts) > 1:
                index_part = parts[1].split('.')[0]
                return int(index_part)
        except Exception:
            logging.warning(f"Could not parse chunk index from filename: {filepath}. Using alphabetical sort.")
        return filepath # Fallback to alphabetical if parsing fails

    sorted_shard_files = sorted(shard_files, key=sort_key)

    # Create a temporary file to hold the list of shards for FFmpeg concat demuxer
    # use delete=False because FFmpeg might need the file after tempfile closes it
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as list_file:
        list_filepath = list_file.name
        for fpath in sorted_shard_files:
            # FFmpeg concat demuxer requires paths to be escaped if they contain special characters,
            # and relative paths are relative to the list file's directory. Using absolute paths is safer.
            list_file.write(f"file '{os.path.abspath(fpath)}'\n")
        logging.info(f"Created FFmpeg concat list file: {list_filepath}")

    success = False
    try:
        # FFmpeg command for concatenation using the concat demuxer
        # -f concat: Use the concat demuxer
        # -safe 0: Allows concat demuxer to use arbitrary file paths (needed for absolute paths or paths outside current dir)
        # -i list_filepath: Specify the input file list
        # -c copy: Copy the codecs directly without re-encoding (fastest, preserves encoded data)
        # -y: Overwrite output file without asking
        ffmpeg_command = [
            'ffmpeg',
            '-f', 'concat',
            '-safe', '0',
            '-i', list_filepath,
            '-c', 'copy',
            '-y',
            output_filename
        ]

        logging.info(f"Running FFmpeg concatenation command: {' '.join(ffmpeg_command)}")

        # Execute the command using the standard subprocess.run within asyncio.to_thread
        process = await asyncio.to_thread(
            subprocess.run,
            ffmpeg_command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False # Don't raise exception for non-zero exit code immediately
        )

        if process.returncode != 0:
            error_message = process.stderr.decode('utf-8', errors='ignore')
            logging.error(f"FFmpeg concatenation failed: Return code {process.returncode}\n{error_message}")
            print(f"--- Concatenation Failed ---")
            print(f"Error: FFmpeg concatenation failed. Check logs for details.")
            success = False
        else:
            logging.info(f"FFmpeg concatenation successful.")
            print(f"--- Concatenation Successful ---")
            print(f"Concatenated video saved to '{output_filename}'")
            success = True

    except FileNotFoundError:
         logging.error(f"FFmpeg command not found. Is FFmpeg installed and in the system PATH?")
         print(f"--- Concatenation Failed ---")
         print("Error: FFmpeg command not found.")
         success = False
    except Exception as e:
        logging.error(f"Unexpected error during FFmpeg concatenation: {e}", exc_info=True)
        print(f"--- Concatenation Failed ---")
        print(f"Error: Unexpected error during concatenation: {e}")
        success = False
    finally:
        # Clean up the temporary list file
        if os.path.exists(list_filepath):
            os.remove(list_filepath)
            logging.info(f"Cleaned up temporary list file: {list_filepath}")

    return success


async def send_video_segments(stub: replication_pb2_grpc.VideoProcessingServiceStub, worker_address: str, video_path: str, target_width: int, target_height: int, output_dir: str):
    """Segments a video, sends segments as chunks to the worker, and retrieves processed shards."""
    video_id = os.path.basename(video_path)
    print(f"--- Processing Video '{video_id}' ({video_path}) ---")
    print(f"Target Resolution: {target_width}x{target_height}")

    if not os.path.exists(video_path):
        print(f"Error: Video file not found at '{video_path}'")
        return [] # Return empty list if file not found

    retrieved_shard_locations = [] # List to store local paths of retrieved shards
    temp_segment_dir = None # Variable to hold the path of the temporary segmentation directory

    try:
        # Use a temporary directory for client-side segmentation output
        with tempfile.TemporaryDirectory() as temp_segment_dir:
            print(f"Using temporary directory for segmentation: {temp_segment_dir}")
            # Segment the video by keyframes
            segment_files = await segment_video_by_keyframes(video_path, temp_segment_dir)

            if not segment_files:
                print("Video segmentation failed or produced no segments. Cannot proceed.")
                return []

            total_segments = len(segment_files)
            successful_chunks_processed = 0
            successful_shards_retrieved = 0
            start_time = time.monotonic()

            # Iterate through each segment file and send its content as a chunk
            for chunk_index, segment_path in enumerate(segment_files):
                print(f"\nSending segment {chunk_index + 1}/{total_segments} (File: {os.path.basename(segment_path)})...")

                try:
                    # Read the entire segment file content as chunk data
                    chunk_data = await asyncio.to_thread(lambda: open(segment_path, "rb").read())
                    chunk_size = len(chunk_data)
                    print(f"  Segment size: {chunk_size} bytes")

                    # Include target_width and target_height in the request
                    request = replication_pb2.ProcessChunkRequest(
                        video_id=video_id,
                        chunk_index=chunk_index, # Use segment index as chunk index
                        chunk_data=chunk_data,
                        target_width=target_width,
                        target_height=target_height
                    )

                    try:
                        # Increased timeout for worker processing (encoding)
                        process_response = await stub.ProcessChunk(request, timeout=90.0)

                        if process_response.success:
                            print(f"  Segment {chunk_index + 1} processed successfully by worker {process_response.worker_id}. Shard at: {process_response.shard_location} (Encoded Size: {process_response.encoded_size} bytes)")
                            successful_chunks_processed += 1

                            # --- Retrieve the processed shard ---
                            if process_response.shard_location:
                                local_shard_path = await retrieve_and_save_shard(stub, process_response.shard_location, output_dir)
                                if local_shard_path:
                                    successful_shards_retrieved += 1
                                    retrieved_shard_locations.append(local_shard_path) # Add local path to list
                            else:
                                 print(f"  Segment {chunk_index + 1} processed but no shard_location provided for retrieval.")

                        else:
                            print(f"  Segment {chunk_index + 1} failed processing by worker {process_response.worker_id}. Message: {process_response.message}")

                    except grpc.aio.AioRpcError as e:
                        print(f"  Segment {chunk_index + 1} RPC failed: {e.details()} (Code: {e.code()})")
                    except Exception as e:
                        print(f"  Segment {chunk_index + 1} failed with unexpected error: {e}")

                    # Add a small delay between sending segments
                    await asyncio.sleep(0.1)

                except IOError as e:
                    logging.error(f"Error reading segment file '{segment_path}': {e}")
                    print(f"  Skipping segment {chunk_index + 1} due to file read error.")
                except Exception as e:
                    logging.error(f"An unexpected error occurred while processing segment {chunk_index + 1}: {e}", exc_info=True)
                    print(f"  Skipping segment {chunk_index + 1} due to unexpected error.")


            end_time = time.monotonic()
            elapsed_time = end_time - start_time

            print(f"\n--- Processing Summary for Video '{video_id}' (Segmented) ---")
            print(f"Total Segments Sent: {total_segments}")
            print(f"Successful Segments Processed (Worker): {successful_chunks_processed}")
            print(f"Successful Shards Retrieved (Client): {successful_shards_retrieved}")
            print(f"Failed Segments: {total_segments - successful_chunks_processed}")
            print(f"Total Time: {elapsed_time:.2f} seconds")
            if elapsed_time > 0 and total_segments > 0:
                 print(f"Average time per segment (send + process + retrieve): {(elapsed_time / total_segments):.4f} seconds")

            return retrieved_shard_locations # Return the list of local shard paths

    except Exception as e:
        logging.error(f"An error occurred during segmentation or segment processing: {e}", exc_info=True)
        return []
    # TemporaryDirectory context manager automatically cleans up temp_segment_dir


async def main():
    parser = argparse.ArgumentParser(description='Video Processing Worker Test Client (Segmentation, Retrieval & Concatenation)')
    parser.add_argument('--worker', type=str, required=True, help='Address of the worker to connect to (e.g., localhost:50061)')
    parser.add_argument('--video_path', type=str, required=True, help='Path to the video file to process')
    # Chunk size is no longer used for splitting, but kept as a potential argument if needed elsewhere
    parser.add_argument('--chunk_size', type=int, default=1024 * 1024, help='(Ignored for splitting) Size of chunks sent to worker in bytes (default: 1MB)')
    parser.add_argument('--output_dir', type=str, default='retrieved_shards', help='Directory to save retrieved shard files')
    parser.add_argument('--target_width', type=int, default=640, help='Target width for encoded video shards')
    parser.add_argument('--target_height', type=int, default=480, help='Target height for encoded video shards')
    parser.add_argument('--concatenated_output', type=str, default='concatenated_output.mp4', help='Filename for the final concatenated video')


    args = parser.parse_args()

    # Use insecure channel for simplicity
    async with grpc.aio.insecure_channel(args.worker) as channel:
        # Create a stub for the VideoProcessingService
        stub = replication_pb2_grpc.VideoProcessingServiceStub(channel)

        # Segment video, send segments, get responses, and retrieve shards
        # Use the new send_video_segments function
        retrieved_shards = await send_video_segments(
            stub,
            args.worker,
            args.video_path,
            args.target_width,
            args.target_height,
            args.output_dir
        )

        # Concatenate retrieved shards if any were successful
        if retrieved_shards:
            # Ensure the output directory exists for the concatenated file
            os.makedirs(os.path.dirname(args.concatenated_output) or '.', exist_ok=True)
            await concatenate_shards(retrieved_shards, args.concatenated_output)
        else:
            print("\nNo shards were successfully retrieved. Skipping concatenation.")


    # Optional: Clean up retrieved shard files after concatenation (or keep them for inspection)
    # import shutil
    # if os.path.exists(args.output_dir):
    #     print(f"\nCleaning up retrieved shard directory: {args.output_dir}")
    #     shutil.rmtree(args.output_dir)


if __name__ == '__main__':
    # Ensure grpcio, protobuf, and ffmpeg-python are installed: pip install grpcio protobuf ffmpeg-python
    # Ensure your .proto file is compiled: python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. replication.proto
    # Make sure FFmpeg is installed on the client machine and in PATH.
    # Make sure worker.py is running before running this client.
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nClient interrupted.")
    except Exception as e:
        logging.error(f"Client main execution failed: {e}", exc_info=True)

