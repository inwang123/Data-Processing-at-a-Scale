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

    # Read schema from the provided JSON file
    with open(header_file) as json_file:
        schema = json.load(json_file)

    # Build CREATE TABLE statement
    columns = [f"{col_name} {data_type}" for col_name, data_type in schema.items()]
    create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"

    cursor.execute(create_sql)

    # Load CSV data
    with open(csv_path, "r") as csv_file:
        cursor.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV HEADER", csv_file)

    connection.commit()
    cursor.close()
    print(f"Loaded {table_name} from {csv_path} using {header_file}")


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
        # first get min/max
        cursor.execute(
            f"SELECT MIN({column_to_partition}), MAX({column_to_partition}) FROM {data_table_name}"
        )
        result = cursor.fetchone()
        min_val = result[0]
        max_val = result[1]

        # calc partition size
        total_range = max_val - min_val
        partition_size = math.ceil(total_range / num_partitions)

        # get columns from headerfile
        with open(header_file) as json_file:
            header_info = json.load(json_file)

        # build columns for table
        column_defs = []
        for col_name, col_type in header_info.items():
            column_defs.append(f"{col_name} {col_type}")

        table_definition = ", ".join(column_defs)

        # parent table
        cursor.execute(
            f"CREATE TABLE {partition_table_name} ({table_definition}) PARTITION BY RANGE ({column_to_partition})"
        )

        # partition tables
        for partition_num in range(num_partitions):
            child_table_name = f"{partition_table_name}{partition_num}"

            # get partition range
            range_start = min_val + partition_num * partition_size
            range_end = min_val + (partition_num + 1) * partition_size

            # include max val
            if partition_num == num_partitions - 1:
                range_end = max_val + 1

            # create partition
            cursor.execute(
                f"CREATE TABLE {child_table_name} PARTITION OF {partition_table_name} FOR VALUES FROM ({range_start}) TO ({range_end})"
            )

        # copy all data to new partition table
        cursor.execute(
            f"INSERT INTO {partition_table_name} SELECT * FROM {data_table_name}"
        )

        connection.commit()
        print(f"Done creating range partitions for {partition_table_name}")

    except Exception as e:
        connection.rollback()
        print("partition failed")
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
        with open(header_file) as json_file:
            headers = json.load(json_file)

        col_defs = []
        for col_name, col_type in headers.items():
            col_defs.append(f"{col_name} {col_type}")

        #  parent table
        create_table_sql = (
            f"CREATE TABLE {partition_table_name} ({', '.join(col_defs)})"
        )
        cursor.execute(create_table_sql)

        # child tables
        for i in range(num_partitions):
            child_table = f"{partition_table_name}{i}"
            cursor.execute(
                f"CREATE TABLE {child_table} () INHERITS ({partition_table_name})"
            )

        cursor.execute(f"SELECT * FROM {data_table_name}")
        rows = cursor.fetchall()

        # Get column names
        columns = list(headers.keys())
        col_string = ", ".join(columns)
        value_placeholders = ", ".join(["%s"] * len(columns))

        # Distribute data
        current_partition = 0
        for row in rows:
            target_table = f"{partition_table_name}{current_partition}"
            insert_sql = f"INSERT INTO {target_table} ({col_string}) VALUES ({value_placeholders})"
            cursor.execute(insert_sql, row)

            current_partition += 1
            if current_partition >= num_partitions:
                current_partition = 0

            # trigger function for new inserts
            trigger_function_code = f"""
            CREATE OR REPLACE FUNCTION {partition_table_name}_insert_trigger()
            RETURNS TRIGGER AS $$
            DECLARE
                min_row_count BIGINT;
                target_partition INTEGER := 0;
                current_row_count BIGINT;
                partition_num INTEGER;
            BEGIN
                --check how many rows are in the first partition
                EXECUTE 'SELECT COUNT(*) FROM {partition_table_name}0' INTO min_row_count;
                
                -- Look through other partitions->find one w fewest rows
                FOR partition_num IN 1..{num_partitions - 1} LOOP
                    EXECUTE 'SELECT COUNT(*) FROM {partition_table_name}' || partition_num INTO current_row_count;
                    -- If this partition has fewer rows than our current minimum, make it the new target
                    IF current_row_count < min_row_count THEN
                        min_row_count = current_row_count;
                        target_partition = partition_num;
                    END IF;
                END LOOP;
                
                -- insert the new row into partition 
                CASE target_partition
            """
            for part_num in range(num_partitions):
                trigger_function_code += f"        WHEN {part_num} THEN INSERT INTO {partition_table_name}{part_num} VALUES (NEW.*);\n"

            trigger_function_code += """
                END CASE;
                RETURN NULL;  -- We've handled the insert ourselves
            END;
            $$ LANGUAGE plpgsql;
            """

        cursor.execute(trigger_function_code)

        # Create trigger onparent table
        cursor.execute(
            f"""
            CREATE TRIGGER {partition_table_name}_trigger 
            BEFORE INSERT ON {partition_table_name} 
            FOR EACH ROW EXECUTE FUNCTION {partition_table_name}_insert_trigger()
        """
        )

        connection.commit()
        print(f"Round-robin partitions created successfully for {partition_table_name}")

    except Exception as e:
        connection.rollback()
        print(f"Error creating round-robin partitions: {e}")
        raise e
    finally:
        cursor.close()


def delete_partitions(table_name, num_partitions, connection):
    """
    This function in NOT graded and for your own testing convinience.
    Use this function to delete all the partitions that are created by you.

    Args:
        table_name (str): The name of the table containing the partitions to be deleted.
        num_partitions (int): The number of partitions to be deleted.
        connection: The database connection object.
    """

    # TODO: UNGRADED: Implement code to delete partitions here
    raise Exception("Function yet to be implemented!")
