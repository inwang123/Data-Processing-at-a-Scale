# NoSQL Key-Value Store with RocksDB

## Overview

Built a NoSQL key-value storage system using RocksDB to efficiently ingest, store, and retrieve structured data from CSV datasets.

The project focuses on scalable data storage techniques, batch processing, and high-performance retrieval operations commonly used in modern distributed databases.

## Features

### Data Ingestion
- Parsed structured CSV datasets and converted records into key-value pairs
- Generated composite keys using unique identifiers and column names
- Loaded large datasets into RocksDB using batch write operations

### Bulk Write Optimization
- Implemented `WriteBatch` operations to improve insertion performance
- Reduced disk I/O overhead by grouping writes into batches
- Processed large datasets efficiently through incremental batch loading

### Query Operations
- Implemented multi-key retrieval using RocksDB `MultiGet`
- Supported efficient retrieval of multiple records in a single operation
- Filtered and processed results dynamically

### Range Scanning
- Built range-based iteration using RocksDB iterators
- Traversed key ranges efficiently
- Supported selective retrieval of matching records

### Database Operations
- Implemented key deletion functionality
- Added status checking and error handling for database operations
- Maintained data consistency during updates and removals

## Technologies

- C++
- RocksDB
- CSV parsing
- Makefile

## System Workflow

1. Read and parse CSV data
2. Convert rows into key-value structures
3. Batch insert records into RocksDB
4. Execute retrieval operations
5. Perform range scans and filtering
6. Handle updates and deletions

## Key Concepts

- NoSQL databases
- Key-value storage systems
- Batch processing
- Range scanning
- Multi-key retrieval
- Data ingestion pipelines
- Database optimization

## Learning Outcomes

- NoSQL architecture principles
- RocksDB database operations
- Efficient data ingestion techniques
- Storage optimization
- Iterator-based retrieval
- High-performance data processing