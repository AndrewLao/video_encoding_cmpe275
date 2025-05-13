import asyncio
import sys
import os

# Define the commands for each worker node
# Make sure the path to node.py is correct if it's not in the current directory
WORKER_COMMANDS = [
    [sys.executable, 'node.py', '--role', 'worker', '--host', 'localhost', '--port', '50061', '--master', 'localhost:50051', '--nodes', 'localhost:50051,localhost:50062,localhost:50063'],
    [sys.executable, 'node.py', '--role', 'worker', '--host', 'localhost', '--port', '50062', '--master', 'localhost:50051', '--nodes', 'localhost:50051,localhost:50061,localhost:50063'],
    [sys.executable, 'node.py', '--role', 'worker', '--host', 'localhost', '--port', '50063', '--master', 'localhost:50051', '--nodes', 'localhost:50051,localhost:50061,localhost:50062'],
]

async def read_stream(stream, prefix):
    """Reads lines from a stream and prints them with a prefix."""
    while True:
        line = await stream.readline()
        if not line:
            break
        # Decode the bytes to string and print with prefix
        print(f"[{prefix}] {line.decode().strip()}")
        await asyncio.sleep(0.001) # Yield control to allow other tasks to run

async def run_worker(command, prefix):
    """Starts a worker process and streams its output."""
    process = None
    try:
        print(f"Starting worker: {' '.join(command)}")
        # Create the subprocess, capturing stdout and stderr
        process = await asyncio.create_subprocess_exec(
            *command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        # Create tasks to read from stdout and stderr concurrently
        stdout_task = asyncio.create_task(read_stream(process.stdout, f"{prefix}-stdout"))
        stderr_task = asyncio.create_task(read_stream(process.stderr, f"{prefix}-stderr"))

        # Wait for the process to finish AND the output streams to be fully read
        await asyncio.gather(process.wait(), stdout_task, stderr_task)

        print(f"Worker {prefix} finished with return code: {process.returncode}")

    except FileNotFoundError:
        print(f"Error: Could not find the executable for command: {command[0]}")
    except Exception as e:
        print(f"An error occurred running worker {prefix}: {e}")
    finally:
        if process and process.returncode is None:
             # If the process is still running, terminate it
             try:
                 process.terminate()
                 await asyncio.wait_for(process.wait(), timeout=5.0) # Wait a bit for termination
             except asyncio.TimeoutError:
                 process.kill() # If termination fails, kill
                 print(f"Worker {prefix} did not terminate gracefully, killed.")
             except Exception as e:
                  print(f"Error during worker {prefix} cleanup: {e}")


async def main():
    """Main function to start all worker nodes concurrently."""
    print("Starting all worker nodes...")

    # Create a list of tasks to run each worker
    worker_tasks = []
    for i, command in enumerate(WORKER_COMMANDS):
        # Use the port number as a simple prefix for logs
        port = command[7] # Assuming port is the 8th element in the command list
        worker_tasks.append(asyncio.create_task(run_worker(command, f"Worker {port}")))

    # Wait for all worker tasks to complete
    await asyncio.gather(*worker_tasks)

    print("All worker nodes have finished.")

if __name__ == '__main__':
    # Ensure node.py is in the same directory or provide the correct path
    if not os.path.exists('node.py'):
        print("Error: node.py not found in the current directory.")
        sys.exit(1)

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped by user.")

