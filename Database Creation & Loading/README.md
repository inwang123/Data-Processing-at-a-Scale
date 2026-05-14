# PostgreSQL Reddit Data Ingestion System

## Overview

Built a scalable PostgreSQL database system to ingest, organize, and manage large-scale Reddit datasets derived from the Pushshift archive.

The project focuses on efficient database creation, schema design, constraint implementation, and optimized loading of large datasets using PostgreSQL and pg_bulkload.

## Features

- Designed relational schemas for:
  - Submissions
  - Comments
  - Authors
  - Subreddits

- Implemented:
  - Primary keys
  - Foreign keys
  - Data constraints
  - Relationship mapping

- Built an automated shell-based workflow for:
  - Database creation
  - Bulk data ingestion
  - Constraint creation
  - Validation and record counting

- Optimized loading performance using:
  - PostgreSQL COPY operations
  - pg_bulkload for high-volume insertion

## Technologies

- PostgreSQL
- SQL
- Bash
- pg_bulkload

## Dataset

Dataset derived from the Pushshift Reddit archive.

Approximate scale:

- 1.2M+ submissions
- 10.5M+ comments
- 6.1M+ authors
- 900K+ subreddits

## System Workflow

1. Create database schema
2. Load CSV datasets
3. Apply constraints and relationships
4. Optimize insertion performance
5. Validate table contents

## Learning Outcomes

- Relational schema design
- Large-scale data ingestion
- Query optimization
- Database constraints
- Performance tuning
- ETL workflow design