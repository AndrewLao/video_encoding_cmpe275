## Worker-Node Folder Components

- `worker.py`: Implements the Video Processing Worker service. It listens for incoming video chunks, processes them, and stores them.

- `test_worker_client.py`: A client script to test the video processing worker. It can read a video file, chunk it, send chunks to a worker, and retrieve the processed shards.

- `video_processing.proto`: Defines the gRPC service (`VideoProcessingService`) and the message structures used for communication between the Master (test client) and the Worker.

- `setup_env.sh`: A convenience script to set up a Python virtual environment, install dependencies, and compile _only_ the `video_processing.proto` file.

## Setup and Installation

1.  Clone or download the project files: Ensure you have the Python scripts (`worker.py`, `test_worker_client.py`), the Protobuf definition file (`video_processing.proto`), and the setup script (`setup_env.sh`) in the same directory.

2.  Make the setup script executable:

    ```
    chmod +x ./setup_env.sh

    ```

3.  Run the setup and compile script: This script will create a Python virtual environment (`venv`), install all necessary libraries (`grpcio`, `protobuf`, `grpcio-tools`), and compile your `video_processing.proto` file.

    ```
    ./setup_env.sh

    ```

4.  Activate the virtual environment: After running the script, your current terminal session should have the virtual environment activated. If you open a new terminal, you'll need to activate it manually:

    ```
    source ./venv/bin/activate

    ```

    You should see `(venv)` at the start of your terminal prompt, indicating the environment is active.

5.  Verify Protobuf Compilation: Check that `video_processing_pb2.py`, `video_processing_pb2_grpc.py`, and `video_processing_pb2.pyi` files have been created in your project directory. If the `setup_env.sh` script reported compilation errors, fix the issues in `video_processing.proto` and re-run the script.

## Running the System

Make sure your virtual environment is activated in each terminal session where you run a component.

### 1\. Start the Video Processing Worker (`worker.py`)

Run one or more instances of `worker.py`. Each worker needs a host and port to listen on.

Example (running a worker on port 50061):

```
source ./venv/bin/activate
python worker.py --host localhost --port 50061

```

### 2\. Use the Test Client (`test_worker_client.py`)

The test client simulates a Master sending video chunks to a worker and retrieving the processed shards. You'll need a video file for testing.

- Create a Test Video File (Optional): If you don't have a small video file, you can create one using `ffmpeg` (install `ffmpeg` if you don't have it):

  ```
  # Example using ffmpeg to create a dummy video
  ffmpeg -f lavfi -i testsrc=duration=5:size=320x240:rate=15 -f lavfi -i sine=duration=5:frequency=220 -c:v libx264 -c:a aac -strict experimental -shortest test_video_small.mp4

  ```

- Run the Test Client: Specify the worker's address, the path to your video file, the chunk size, and an output directory for retrieved shards.

  ```
  source ./venv/bin/activate
  python test_worker_client.py --worker localhost:50061 --video_path ./test_video_small.mp4 --chunk_size 524288 --output_dir ./retrieved_shards

  ```

  (Adjust `--worker`, `--video_path`, `--chunk_size` (e.g., 524288 for 512KB), and `--output_dir` as needed).

## Testing and Verification

- Worker Logs: Check the terminal where you ran `worker.py`. You should see logs indicating that it received chunks, simulated encoding, and stored shards in the `video_shards` directory (created in the same directory as `worker.py`).

- Client Output: The `test_worker_client.py` output will show the progress of sending chunks and retrieving shards. Look for "Chunk X processed successfully" and "Successfully retrieved and saved shard data".

- Retrieved Shards: Check the directory specified by `--output_dir` (e.g., `./retrieved_shards`). You should find files corresponding to the encoded chunks (e.g., `test_video_small.mp4_chunk_0.shard`, `test_video_small.mp4_chunk_1.shard`).

- Verify Shard Content (Optional): While the simulated encoding doesn't produce a standard video format, you can inspect the retrieved `.shard` files. Their size should be smaller than the original chunks. You can also try concatenating them (`cat retrieved_shards/*.shard > reconstructed.bin`) and attempting to open the `reconstructed.bin` file with a tolerant player like VLC, although it's unlikely to play correctly due to the simulated encoding.

## Further Development

- Integrate a real video encoding library (like `ffmpeg`) into the `worker.py`'s `ProcessChunk` method to produce actual encoded video segments.

- Develop a Master process that handles client video uploads, chunks the video, selects workers (potentially using a load balancing or simple adaptive strategy for workers), sends chunks, and manages the metadata of the stored shards.

- Implement a mechanism for the Master to reconstruct the full video from the stored shards based on client requests.

