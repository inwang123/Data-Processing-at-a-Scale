# Graph Data Processing with Neo4j

## Overview

Built a Dockerized graph processing system using Neo4j to load and analyze real-world transportation data from the NYC Taxi dataset.

The project focuses on graph database design, graph analytics, and algorithm implementation using Neo4j Graph Data Science (GDS).

## Features

### Graph Database Construction
- Built a Dockerized Neo4j environment for reproducible deployment
- Loaded and transformed NYC Taxi trip data into graph structures
- Modeled transportation data using graph nodes and relationships

### Graph Schema Design

Nodes:
- `Location`
    - Property:
        - `name`

Relationships:
- `TRIP`
    - Properties:
        - `distance`
        - `fare`
        - `pickup_dt`
        - `dropoff_dt`

### Graph Algorithms

#### PageRank
- Implemented PageRank using Neo4j Graph Data Science
- Evaluated node importance using weighted graph relationships
- Returned highest and lowest ranking nodes

#### Breadth First Search (BFS)
- Implemented BFS traversal between graph locations
- Supported searches from a source node to multiple targets
- Traversed nodes based on shortest-distance exploration

## Technologies

- Neo4j
- Cypher
- Docker
- Python
- Graph Data Science (GDS)
- Pandas

## System Workflow

1. Build Dockerized Neo4j environment
2. Load NYC Taxi dataset
3. Transform trip records into graph structures
4. Create nodes and relationships
5. Execute graph algorithms
6. Analyze graph traversal and ranking results

## Key Concepts

- Graph databases
- Graph traversal
- PageRank
- Breadth First Search
- Dockerized deployment
- Graph analytics
- Transportation network modeling

## Learning Outcomes

- Neo4j graph modeling
- Cypher query development
- Graph algorithm implementation
- Containerized environments
- Large-scale graph processing