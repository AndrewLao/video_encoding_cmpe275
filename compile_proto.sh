#!/bin/bash
# compile_proto.sh - Compiles the protobuf definitions

# Ensure we're in the Worker directory
cd "$(dirname "$0")" || exit 1

# Activate the virtual environment if it exists
if [ -d "./venv" ]; then
    source ./venv/bin/activate
    echo "Activated virtual environment"
else
    echo "Warning: Virtual environment not found. You may need to run setup_env.sh first."
fi

# Compile the protobuf definitions
echo "Compiling replication.proto..."
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. --pyi_out=. replication.proto

if [ $? -eq 0 ]; then
    echo "Successfully compiled replication.proto"
    echo "Generated files:"
    ls -la replication_pb2*.py*
else
    echo "Failed to compile replication.proto"
    exit 1
fi

echo "Compilation completed successfully."
