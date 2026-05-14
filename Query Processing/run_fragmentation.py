# run_fragmentation.py
import fragmentation
import psycopg2


def main():
    # Database connection
    conn = fragmentation.get_open_connection()

    try:
        # Load data
        print("Loading data...")
        fragmentation.load_data(
            table_name="subreddits",
            csv_path="./subreddits.csv",  # Adjust path if needed
            connection=conn,
            header_file="./headers.json",
        )

        # Create range partitions
        print("Creating range partitions...")
        fragmentation.range_partition(
            data_table_name="subreddits",
            partition_table_name="range_part",
            num_partitions=5,
            header_file="./headers.json",
            column_to_partition="created_utc",
            connection=conn,
        )

        # Create round-robin partitions
        print("Creating round-robin partitions...")
        fragmentation.round_robin_partition(
            data_table_name="subreddits",
            partition_table_name="rrobin_part",
            num_partitions=5,
            header_file="./headers.json",
            connection=conn,
        )

        print("Fragmentation completed successfully!")

    except Exception as e:
        print(f"Error during fragmentation: {e}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
