#!/bin/bash

# Exit immediately if a command exits with a non-zero status.
# This is important so the script stops if venv setup or pip install fails.
set -e

# Navigate to the directory containing the script.
# This ensures the virtual environment, dependencies, and generated proto files
# are all located in the correct place relative to your project files.
cd "$(dirname "$0")"

echo "--- Setting up Python virtual environment and installing dependencies ---"

# Define the name of the virtual environment directory
VENV_DIR="venv"

# Check if the virtual environment directory already exists
if [ ! -d "$VENV_DIR" ]; then
  echo "Creating virtual environment in ./$VENV_DIR"
  # Create a new virtual environment using python3
  python3 -m venv "$VENV_DIR"
else
  echo "Virtual environment already exists in ./$VENV_DIR"
fi

# Activate the virtual environment
# The 'source' command is used to run the activation script in the current shell.
# This makes the 'python' and 'pip' commands point to the ones inside the venv.
echo "Activating virtual environment..."
source "$VENV_DIR/bin/activate"

# Ensure pip is up-to-date within the virtual environment
echo "Upgrading pip..."
pip install --upgrade pip

# Install the required dependencies
# These are the libraries needed for gRPC communication, protobuf handling, and system metrics (psutil).
# grpcio-tools is needed for the protoc command.
echo "Installing project dependencies (grpcio, protobuf, grpcio-tools, psutil)..."
pip install grpcio protobuf grpcio-tools psutil

echo "Dependencies installed."

echo "--- Compiling Protobuf file ---"

# Check if the .proto file exists before attempting to compile
PROTO_FILE="replication.proto"
if [ ! -f "$PROTO_FILE" ]; then
  echo "Error: Protobuf file '$PROTO_FILE' not found in the current directory."
  echo "Please ensure '$PROTO_FILE' exists before running this script."
  exit 1 # Exit with an error code
fi

# Run the protoc command to generate Python code from the .proto file.
# -I. tells protoc to look for imports (like timestamp.proto) in the current directory.
# --python_out=. generates standard Python classes.
# --pyi_out=. generates Python interface files (.pyi) for type hinting (optional but good).
# --grpc_python_out=. generates the gRPC service client and server classes.
echo "Running protoc compiler on $PROTO_FILE..."
python -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpc_python_out=. "$PROTO_FILE"

# Check the exit status of the protoc command
if [ $? -eq 0 ]; then
  echo "Protobuf compilation successful."
  echo "Generated files: replication_pb2.py, replication_pb2_grpc.py, replication_pb2.pyi"
else
  echo "Error: Protobuf compilation failed."
  echo "Please check the error messages above for details on why protoc failed."
  # The 'set -e' at the top should handle this, but an explicit message is helpful.
fi

echo "--- Setup and Compilation Complete ---"
echo "To use this virtual environment in a new terminal, navigate to this directory and run: source ./$VENV_DIR/bin/activate"
echo "You can now run your server, worker, and client scripts using 'python <script_name>.py'."

