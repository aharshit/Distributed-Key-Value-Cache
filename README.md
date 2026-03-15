# Distributed Key-Value Cache

This is a distributed, log-structured merge (LSM) tree key-value store project. It is built to support efficient storage and retrieval of key-value data across several distributed nodes.

## Features
- LSM tree architecture with an in-memory sorted structure and on-disk stored string tables.
- Node partitioning with consistent hashing.
- High availability with inter-node communication using gRPC.
- Data rebalancing.

## Quick Start
You'll need Go 1.24+ and Protocol Buffers to build this project.

1. Ensure protobuf and plugins are installed:
   ```bash
   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
   ```
2. Build and run:
   ```bash
   make run
   ```

A server will open on port `:8080`, routing requests to the distributed nodes.
