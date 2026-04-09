# Import required libraries
import traceback
import psycopg2
import psycopg2.extras
import csv
import json
import math


########################## Setup Functions ##########################

def get_open_connection(username='postgres', password='postgres', dbname='postgres', host="127.0.0.1"):
    """
    Connect to the database and return connection object
    
    Returns:
        connection: The database connection object.
    """

    return psycopg2.connect(f"dbname='{dbname}' user='{username}' host='{host}' password='{password}'")


def create_db(dbname):
    """
    Create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    """

    # Connect to the database with default values
    con = get_open_connection()
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute(f"select count(*) from pg_catalog.pg_database where datname='{dbname}'")
    if cur.fetchone()[0] == 0:
        cur.execute(f'create database {dbname}')  # Create the database
    else:
        print(f'A database named "{dbname}" already exists')

    # Clean up
    cur.close()
    con.close()


def delete_db(dbname):
    """
    Let it go...  
    Deleting the database provided to you
    """

    con = get_open_connection()
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    cur.execute(f'drop database if exists {dbname} (force)')
    
    cur.close()
    con.close()


def delete_specific_table(table_name, connection):
    """
    Deleting a specific table from the database
    """

    cur = connection.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    connection.commit()
    cur.close()


def delete_all_public_tables(connection):
    """
    Deleting all the tables in the database 
    """

    cur = connection.cursor()
    table_list = []

    cur.execute("select table_name from information_schema.tables where table_schema = 'public'")
    for row in cur:
        table_list.append(row[0])

    for table in table_list:
        cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")
        connection.commit()

    cur.close()


#####################################################################



########################### Data Loading ############################

def load_data(table_name, csv_path, header_file, connection):
    """
    Create a table with the given name and load data from the CSV file located at the given path.

    NOTE: This function is NOT needed for query_processing assignment as data is already loaded.
    It is kept here only for compatibility with the tester structure.

    Args:
        table_name (str): The name of the table where data is to be loaded.
        csv_path (str): The path to the CSV file containing the data to be loaded.
        header_file (str): The path to where the header file is located
        connection: The database connection object.
    """

    # This function is not used in query_processing assignment
    # Data is already loaded by the fragmentation assignment
    pass


def range_partition(data_table_name, partition_table_name, num_partitions, header_file, column_to_partition, connection):
    """
    Use this function to partition the data in the given table using a range partitioning approach.

    NOTE: This function is NOT needed for query_processing assignment as partitions already exist.
    It is kept here only for compatibility with the tester structure.

    Args:
        data_table_name (str): The name of the table that contains the data loaded during load_data phase.
        partition_table_name (str): The name of the table to be partitioned.
        num_partitions (int): The number of partitions to create.
        header_file (str): path to the header file that contains column headers and their data types
        column_to_partition (str): The column based on which we are creating the partition.
        connection: The database connection object.
    """

    # This function is not used in query_processing assignment
    # Partitions are created by the fragmentation assignment
    pass


def round_robin_partition(data_table_name, partition_table_name, num_partitions, header_file, connection):
    """
    Use this function to partition the data in the given table using a round-robin approach.

    NOTE: This function is NOT needed for query_processing assignment as partitions already exist.
    It is kept here only for compatibility with the tester structure.

    Args:
        table_name (str): The name of the table to be partitioned.
        num_partitions (int): The number of partitions to create.
        connection: The database connection object.
    """

    # This function is not used in query_processing assignment
    # Partitions are created by the fragmentation assignment
    pass

#####################################################################


########################### The Real Deal ###########################

def test_point_query(expected_output_file, table_to_test, connection):
    """
    Function to test the point query. 
    Selects table from the db and compares it to data from a text file.

    Args:
        expected_output_file (str): Name of the file containing the expected output.
        table_to_test (str): Name of the table containing the data.
        connection: Connection object for the database.

    Returns:
        bool: A boolean value indicating whether the tests passed or failed.
    """

    with open(expected_output_file, 'r', encoding="utf-8") as f:
        reader = csv.reader(f)
        expected_output = list(reader)

    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM {table_to_test}")
    actual_output = cursor.fetchall()

    if len(expected_output) != len(actual_output):
        return False, f"Expected {len(expected_output)} rows, got {len(actual_output)} rows"
    
    for i in range(len(expected_output)):

        actual_output_list = [str(x) if x is not None else '' for x in actual_output[i]]
        
        for j in range(0, len(expected_output[i])):
            if expected_output[i][j] != actual_output_list[j]:
                return False, f"Data in row {i + 1} and column {j + 1} doesn't match"
        
    return True, None

    

def test_range_query(expected_output_file, table_to_test, connection):
    """
    Function to test the range query. Selects table from the db and compares it to data from a text file.

    Args:
        my_assignment: Object containing the pointQuery method to be tested.
        expected_output_file (str): Name of the file containing the expected output.
        connection: Connection object for the database.

    Returns:
        bool, Error: A list containing a boolean value indicating whether the tests passed or failed, and an error message if the tests failed.
    """
    
    with open(expected_output_file, 'r', encoding="utf-8") as f:
        reader = csv.reader(f)
        expected_output = list(reader)

    cursor = connection.cursor()
    cursor.execute(f"SELECT * FROM {table_to_test}")
    actual_output = cursor.fetchall()

    if len(expected_output) != len(actual_output):
        return False, f"Expected {len(expected_output)} rows, got {len(actual_output)} rows"
    
    for i in range(len(expected_output)):

        actual_output_list = [str(x) if x is not None else '' for x in actual_output[i]]
        
        for j in range(0, len(expected_output[i])):
            if expected_output[i][j] != actual_output_list[j]:
                return False, f"Data in row {i + 1} and column {j + 1} doesn't match"
        
    return True, None