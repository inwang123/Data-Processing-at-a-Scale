#!/usr/bin/env bash
set -euo pipefail

# Connect and load schema here
export PGHOST=127.0.0.1 PGPORT=5432 
export PGUSER=postgres PGPASSWORD=postgres PGDATABASE=postgres
psql -f schema.sql


# Load submissions, if pg bulkload fails then fallback to the COPY command
if command -v pg_bulkload &> /dev/null && pg_bulkload -i submissions.csv -O submissions -o "TYPE=CSV" -o "SKIP=1" -o "DELIMITER=," -o "NULL="; then
    echo "Loaded submissions with pg_bulkload"
else
    echo "Using COPY for submissions "
    psql -c "COPY submissions FROM 'submissions.csv' WITH (FORMAT csv, HEADER true, NULL '');"
fi

if command -v pg_bulkload &> /dev/null && pg_bulkload -i comments.csv -O comments -o "TYPE=CSV" -o "SKIP=1" -o "DELIMITER=," -o "NULL="; then
    echo "Loaded comments with pg_bulkload"
else
    echo "Using COPY for comments"
    psql -c "COPY comments FROM 'comments.csv' WITH (FORMAT csv, HEADER true, NULL '');"
fi

if command -v pg_bulkload &> /dev/null && pg_bulkload -i authors.csv -O authors -o "TYPE=CSV" -o "SKIP=1" -o "DELIMITER=," -o "NULL="; then
    echo "Loaded authors with pg_bulkload"
else
    echo "Using COPY for authors"
    psql -c "COPY authors FROM 'authors.csv' WITH (FORMAT csv, HEADER true, NULL '');"
fi

if command -v pg_bulkload &> /dev/null && pg_bulkload -i subreddits.csv -O subreddits -o "TYPE=CSV" -o "SKIP=1" -o "DELIMITER=," -o "NULL="; then
    echo "Loaded subreddits with pg_bulkload"
else
    echo "Using COPY for subreddits"
    psql -c "COPY subreddits FROM 'subreddits.csv' WITH (FORMAT csv, HEADER true, NULL '');"
fi

# Add constraints
psql -f add_constraints.sql

# View table + count
psql <<SQL
SELECT 'subreddits' as table, COUNT(*) FROM subreddits
UNION ALL SELECT 'authors', COUNT(*) FROM authors  
UNION ALL SELECT 'submissions', COUNT(*) FROM submissions
UNION ALL SELECT 'comments', COUNT(*) FROM comments;
SQL