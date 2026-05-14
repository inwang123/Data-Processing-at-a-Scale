# PostgreSQL Data Fragmentation & Partitioning System

## Overview

Built a PostgreSQL database system implementing multiple data fragmentation and partitioning strategies for large-scale datasets.

The project focuses on scalable data organization, efficient loading, and automated distribution of records using PostgreSQL partitioning techniques and triggers.

## Features

### Data Loading
- Loaded large CSV datasets into PostgreSQL using bulk loading methods
- Automated schema generation using JSON-based header specifications
- Improved ingestion efficiency using PostgreSQL COPY operations

### Range Partitioning
- Implemented horizontal partitioning using PostgreSQL's native range partitioning
- Computed partition ranges dynamically using dataset statistics
- Automatically routed records into correct partitions

### Round-Robin Partitioning
- Implemented custom round-robin partitioning using:
  - PostgreSQL table inheritance
  - PL/pgSQL triggers
- Created automated distribution logic for ongoing inserts
- Maintained balanced partition sizes across child tables

## Technologies

- PostgreSQL
- SQL
- Python
- PL/pgSQL
- JSON
- CSV data processing

## System Architecture

Workflow:

1. Load source dataset into PostgreSQL
2. Generate database schema
3. Create parent and child partition tables
4. Apply fragmentation strategy
5. Route and distribute records automatically
6. Handle future inserts dynamically

## Key Concepts

- Horizontal fragmentation
- Range partitioning
- Round-robin partitioning
- Database triggers
- Table inheritance
- Data ingestion pipelines
- Performance optimization

## Learning Outcomes

- Database scalability techniques
- PostgreSQL partition management
- Trigger implementation
- Query optimization
- Automated data distribution
- Large-scale data organization