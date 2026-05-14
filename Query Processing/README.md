# Partitioned Query Engine for PostgreSQL

## Overview

Built a simplified query processing system for partitioned PostgreSQL databases capable of executing range and point queries across fragmented datasets.

The project extends previous database partitioning work by implementing query execution logic that retrieves and processes records across multiple partitions while maintaining correctness and ordering.

## Features

### Range Query Processing
- Implemented range-based query execution across partitioned tables
- Retrieved records using configurable lower and upper timestamp boundaries
- Returned sorted results in ascending order by `created_utc`
- Stored query outputs automatically into generated result tables

### Point Query Processing
- Implemented point lookup functionality for exact timestamp matches
- Queried parent partition tables and retrieved matching records
- Generated result tables dynamically for query output storage

### Partition Support
- Supported:
  - Range-partitioned tables
  - Round-robin partitioned tables
- Maintained consistency across partitioning strategies

## Technologies

- PostgreSQL
- SQL
- Python
- Psycopg2

## System Workflow

1. Access parent partition tables
2. Identify required partitions
3. Execute point or range query logic
4. Collect and merge results
5. Sort records where required
6. Store results into generated output tables

## Key Concepts

- Query execution
- Database partitioning
- Range queries
- Point queries
- Query optimization
- Distributed data access
- PostgreSQL table processing

## Learning Outcomes

- Query execution strategies
- Partition-aware data retrieval
- Database optimization concepts
- Result aggregation
- Scalable data processing techniques