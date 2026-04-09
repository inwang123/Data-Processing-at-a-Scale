# Import required libraries
# Do not install/import any additional libraries
import psycopg2
import psycopg2.extras
import json
import csv
import math


# Lets define some of the essentials
# We'll define these as global variables to keep it simple
username = "postgres"
password = "postgres"
dbname = "fragmentation"
host = "127.0.0.1"


def get_open_connection():
    """
    Connect to the database and return connection object

    Returns:
        connection: The database connection object.
    """

    return psycopg2.connect(
        f"dbname='{dbname}' user='{username}' host='{host}' password='{password}'"
    )


def load_data(table_name, csv_path, connection, header_file):
    """
    Create a table with the given name and load data from the CSV file located at the given path.

    Args:
        table_name (str): The name of the table where data is to be loaded.
        csv_path (str): The path to the CSV file containing the data to be loaded.
        connection: The database connection object.
        header_file (str): The path to where the header file is located
    """

    cursor = connection.cursor()

    # Creating the table
    with open(header_file) as json_data:
        header_dict = json.load(json_data)

    table_rows_formatted = ", ".join(
        f"{header} {header_type}" for header, header_type in header_dict.items()
    )
    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {table_rows_formatted}
            )"""

    cursor.execute(create_table_query)
    connection.commit()

    # Load data using COPY command
    try:
        # Open the CSV file and copy data to the table
        with open(csv_path, "r") as f:
            # Use COPY command to load data from CSV
            cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", f)

        connection.commit()
        print(f"Data loaded successfully into {table_name}")

    except Exception as e:
        connection.rollback()
        print(f"Error loading data: {e}")
        raise e
    finally:
        cursor.close()


def range_partition(
    data_table_name,
    partition_table_name,
    num_partitions,
    header_file,
    column_to_partition,
    connection,
):
    """
    Use this function to partition the data in the given table using a range partitioning approach.

    Args:
        data_table_name (str): The name of the table that contains the data loaded during load_data phase.
        partition_table_name (str): The name of the table to be created for partitioning.
        num_partitions (int): The number of partitions to create.
        header_file (str): path to the header file that contains column headers and their data types
        column_to_partition (str): The column based on which we are creating the partition.
        connection: The database connection object.
    """

    cursor = connection.cursor()

    try:
        # Step 1: Get min and max values of the partition column
        cursor.execute(
            f"SELECT MIN({column_to_partition}), MAX({column_to_partition}) FROM {data_table_name}"
        )
        min_val, max_val = cursor.fetchone()

        # Calculate partition gap (round up as per instructions)
        partition_gap = math.ceil((max_val - min_val) / num_partitions)

        # Step 2: Create parent table with range partitioning
        with open(header_file) as json_data:
            header_dict = json.load(json_data)

        table_rows_formatted = ", ".join(
            f"{header} {header_type}" for header, header_type in header_dict.items()
        )

        # Create parent table with partition definition
        create_parent_query = f"""
            CREATE TABLE {partition_table_name} (
                {table_rows_formatted}
            ) PARTITION BY RANGE ({column_to_partition})
        """
        cursor.execute(create_parent_query)

        # Step 3: Create child partition tables
        for i in range(num_partitions):
            child_table_name = f"{partition_table_name}{i}"
            range_start = min_val + i * partition_gap
            range_end = min_val + (i + 1) * partition_gap

            # For the last partition, extend range to include max_val
            if i == num_partitions - 1:
                range_end = max_val + 1  # Add 1 to make it inclusive of max_val

            create_child_query = f"""
                CREATE TABLE {child_table_name} 
                PARTITION OF {partition_table_name}
                FOR VALUES FROM ({range_start}) TO ({range_end})
            """
            cursor.execute(create_child_query)

        # Step 4: Insert data from original table into partitions
        cursor.execute(
            f"INSERT INTO {partition_table_name} SELECT * FROM {data_table_name}"
        )

        connection.commit()
        print(f"Range partitioning completed with {num_partitions} partitions")

    except Exception as e:
        connection.rollback()
        print(f"Error in range partitioning: {e}")
        raise e
    finally:
        cursor.close()


def round_robin_partition(
    data_table_name, partition_table_name, num_partitions, header_file, connection
):
    """
    Use this function to partition the data in the given table using a round-robin approach.

    Args:
        data_table_name (str): The name of the table that contains the data loaded during load_data phase.
        partition_table_name (str): The name of the table to be created for partitioning.
        num_partitions (int): The number of partitions to create.
        header_file (str): path to the header file that contains column headers and their data types
        connection: The database connection object.
    """

    cursor = connection.cursor()

    try:
        # Step 1: Create parent table (not partitioned)
        with open(header_file) as json_data:
            header_dict = json.load(json_data)

        table_rows_formatted = ", ".join(
            f"{header} {header_type}" for header, header_type in header_dict.items()
        )

        create_parent_query = f"""
            CREATE TABLE {partition_table_name} (
                {table_rows_formatted}
            )
        """
        cursor.execute(create_parent_query)

        # Step 2: Create child tables using inheritance
        for i in range(num_partitions):
            child_table_name = f"{partition_table_name}{i}"
            create_child_query = f"""
                CREATE TABLE {child_table_name} () 
                INHERITS ({partition_table_name})
            """
            cursor.execute(create_child_query)

        # Step 3: Initial data distribution using round-robin
        # Add row number and distribute based on modulo
        columns = list(header_dict.keys())
        columns_str = ", ".join(columns)

        # Use row_number() to distribute data evenly
        for i in range(num_partitions):
            cursor.execute(
                f"""
                INSERT INTO {partition_table_name}{i} ({columns_str})
                SELECT {columns_str} 
                FROM (
                    SELECT *, 
                           (ROW_NUMBER() OVER () - 1) % {num_partitions} as rn
                    FROM {data_table_name}
                ) t 
                WHERE rn = {i}
            """
            )

        # Step 4: Create trigger function for ongoing inserts
        # Create function to find least filled partition
        trigger_function_query = f"""
            CREATE OR REPLACE FUNCTION {partition_table_name}_insert_trigger()
            RETURNS TRIGGER AS $$
            DECLARE
                min_count BIGINT;
                target_partition INTEGER;
                partition_count BIGINT;
                i INTEGER;
            BEGIN
                -- Initialize with first partition count
                EXECUTE 'SELECT COUNT(*) FROM {partition_table_name}0' INTO min_count;
                target_partition := 0;
                
                -- Check all partitions to find the one with minimum count
        """

        # Add checks for all remaining partitions
        for i in range(1, num_partitions):
            trigger_function_query += f"""
                EXECUTE 'SELECT COUNT(*) FROM {partition_table_name}{i}' INTO partition_count;
                IF partition_count < min_count THEN
                    min_count := partition_count;
                    target_partition := {i};
                END IF;
            """

        trigger_function_query += f"""
                
                -- Insert into the target partition (least-filled)
                CASE target_partition
        """

        # Add cases for all partitions
        for i in range(num_partitions):
            trigger_function_query += f"""
                    WHEN {i} THEN
                        INSERT INTO {partition_table_name}{i} VALUES (NEW.*);
            """

        trigger_function_query += """
                END CASE;
                
                RETURN NULL; -- Suppress the original insert
            END;
            $$ LANGUAGE plpgsql;
        """

        cursor.execute(trigger_function_query)

        # Step 5: Create trigger on parent table
        trigger_query = f"""
            DROP TRIGGER IF EXISTS {partition_table_name}_trigger ON {partition_table_name};
            CREATE TRIGGER {partition_table_name}_trigger
            BEFORE INSERT ON {partition_table_name}
            FOR EACH ROW EXECUTE FUNCTION {partition_table_name}_insert_trigger()
        """
        cursor.execute(trigger_query)

        connection.commit()
        print(f"Round-robin partitioning completed with {num_partitions} partitions")

    except Exception as e:
        connection.rollback()
        print(f"Error in round-robin partitioning: {e}")
        raise e
    finally:
        cursor.close()


def delete_partitions(table_name, num_partitions, connection):
    """
    This function in NOT graded and for your own testing convenience.
    Use this function to delete all the partitions that are created by you.

    Args:
        table_name (str): The name of the table containing the partitions to be deleted.
        num_partitions (int): The number of partitions to be deleted.
        connection: The database connection object.
    """

    cursor = connection.cursor()

    try:
        # Delete child partitions first
        for i in range(num_partitions):
            child_table = f"{table_name}{i}"
            cursor.execute(f"DROP TABLE IF EXISTS {child_table} CASCADE")

        # Delete parent table
        cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")

        # Drop the trigger function if it exists
        cursor.execute(f"DROP FUNCTION IF EXISTS {table_name}_insert_trigger() CASCADE")

        connection.commit()
        print(f"Deleted {num_partitions} partitions for {table_name}")

    except Exception as e:
        connection.rollback()
        print(f"Error deleting partitions: {e}")
        raise e
    finally:
        cursor.close()
