# Shard Storage System Documentation

## Overview

The Shard Storage System is a component of the Worker nodes in the distributed video encoding system. It is responsible for organizing, storing, and managing video chunks after they have been processed by the encoding system.

## Key Components

### 1. Directory Structure

The system organizes processed video shards in a hierarchical directory structure:

```
video_shards/
├── <video_id_1>/
│   ├── chunk_0.shard
│   ├── chunk_1.shard
│   └── chunk_n.shard
├── <video_id_2>/
│   ├── chunk_0.shard
│   ├── chunk_1.shard
│   └── chunk_n.shard
└── metadata/
    ├── <video_id_1>_chunk_0_metadata.json
    ├── <video_id_1>_chunk_1_metadata.json
    ├── <video_id_2>_chunk_0_metadata.json
    └── ...
```

### 2. Shard Files

Each shard file (`chunk_<index>.shard`) contains the encoded data for a specific chunk of a video. The encoding is currently simulated but could be replaced with actual video encoding in a production environment.

### 3. Metadata Storage

For each processed chunk, a metadata file is generated in the `metadata/` directory. The metadata file is a JSON document containing:

```json
{
  "video_id": "example_video.mp4",
  "chunk_index": 0,
  "original_size": 1048576,
  "encoded_size": 524288,
  "compression_ratio": 0.5,
  "worker_id": "localhost:50061",
  "shard_location": "/absolute/path/to/video_shards/example_video.mp4/chunk_0.shard",
  "timestamp": "2023-04-25T14:30:45.123456"
}
```

## API Methods

The Worker node exposes several gRPC methods for interacting with the Shard Storage System:

### 1. ProcessChunk

Processes a video chunk and stores it as a shard. Creates the necessary directory structure and metadata.

```protobuf
rpc ProcessChunk(ProcessChunkRequest) returns (ProcessChunkResponse);
```

### 2. GetShard

Retrieves a specific shard by its location.

```protobuf
rpc GetShard(GetShardRequest) returns (GetShardResponse);
```

### 3. ListShards

Lists all shards stored on the worker, organized by video. If a video ID is provided, lists only shards for that video.

```protobuf
rpc ListShards(ListShardsRequest) returns (ListShardsResponse);
```

### 4. GetVideoMetadata

Retrieves metadata for all chunks of a specific video.

```protobuf
rpc GetVideoMetadata(GetVideoMetadataRequest) returns (GetVideoMetadataResponse);
```

## Lock Mechanism

The system uses multiple lock mechanisms to ensure thread safety:

- `_file_write_lock`: Prevents concurrent write operations to shard files
- `_file_read_lock`: Prevents read operations during write operations
- `_active_tasks_lock`: Protects the active tasks counter
- `_metadata_lock`: Ensures thread-safe operations on metadata files

## Testing and Verification

The system can be tested using the provided testing tools:

1. **test_shard_storage.py**: Tests listing shards, retrieving metadata, and getting shards
2. **test_worker_client.py**: Tests end-to-end processing of a video file

## Integration with Master Node

The Master node interacts with the Shard Storage System by:

1. Sending video chunks to workers for processing
2. Receiving information about where shards are stored
3. Maintaining a map of chunk locations for each video
4. Retrieving shards as needed for video reconstruction

## Future Enhancements

Potential improvements to the Shard Storage System:

1. **Disk Space Management**: Implement policies for managing disk space, such as LRU (Least Recently Used) eviction
2. **Shard Replication**: Add functionality to replicate shards across multiple workers for redundancy
3. **Shard Migration**: Support for migrating shards between workers for load balancing
4. **Persistent Metadata Database**: Replace the JSON file-based metadata with a database for better performance and querying capabilities
5. **Actual Video Encoding**: Replace the simulated encoding with a real video encoder (e.g., using FFmpeg)
