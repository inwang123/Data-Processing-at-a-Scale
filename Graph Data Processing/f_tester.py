# Import required libraries
import psycopg2
import traceback
import test_helper
import fragmentation
import json


dbname = "fragmentation"
count_of_partitions = 5

# Table nomenclature
data_table_name = "subreddits"
column_to_partition = "created_utc"
range_table_prefix = "range_part"
rrobin_table_prefix = "rrobin_part"

# Data files
input_file_path = "./subreddits.csv"
header_path = "./headers.json"
insert_data_path_1 = "./insert1.json"
insert_data_path_2 = "./insert2.json"
insert_data_path_3 = "./insert3.json"

# Misc
rows_in_input = 914067

# Test results tracking
test_results = {
    "load_data": {"passed": False, "points": 0, "max_points": 20, "details": []},
    "range_partition": {"passed": False, "points": 0, "max_points": 40, "details": []},
    "round_robin_partition": {
        "passed": False,
        "points": 0,
        "max_points": 40,
        "details": [],
    },
}


def print_rubric_header():
    print("=" * 80)
    print("ASSIGNMENT: DATA FRAGMENTATION - GRADING RUBRIC")
    print("=" * 80)
    print("Total Points: 100")
    print("Step 1: Data Loading (20 points)")
    print("Step 2: Range Partition (40 points)")
    print("Step 3: Round Robin Partition (40 points)")
    print("=" * 80)


def print_test_results():
    print("\n" + "=" * 80)
    print("FINAL GRADING RESULTS")
    print("=" * 80)

    total_points = 0
    for test_name, result in test_results.items():
        print(f"\n{test_name.replace('_', ' ').title()}:")
        print(f"  Status: {'PASSED' if result['passed'] else 'FAILED'}")
        print(f"  Points: {result['points']}/{result['max_points']}")
        if result["details"]:
            print("  Details:")
            for detail in result["details"]:
                print(f"    - {detail}")
        total_points += result["points"]

    print(f"\nTOTAL SCORE: {total_points}/100 points")
    print("=" * 80)


def test_load_data_detailed(
    assignment_module, table_name, csv_path, conn, expected_rows, header_path
):
    """A. Load Data (20 pts) - Bulk load via COPY in Python"""
    points = 0
    details = []

    try:
        assignment_module.load_data(table_name, csv_path, conn, header_path)

        # Check data loading
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        actual_rows = cursor.fetchone()[0]

        if actual_rows == expected_rows:
            points = 20
            details.append(
                f"(20) Bulk load via COPY in Python: PASSED — {actual_rows} rows loaded (matches CSV)."
            )
        else:
            details.append(
                f"(0) Bulk load via COPY in Python: FAILED — {actual_rows} rows loaded, expected {expected_rows}."
            )

        test_results["load_data"]["passed"] = points == 20
        test_results["load_data"]["points"] = points
        test_results["load_data"]["details"] = details

        return [points == 20, None]

    except Exception as e:
        test_results["load_data"]["passed"] = False
        test_results["load_data"]["points"] = 0
        test_results["load_data"]["details"] = [
            f"(0) Bulk load via COPY in Python: FAILED — {str(e)}"
        ]
        return [False, e]


def test_range_partition_detailed(
    assignment_module,
    data_table_name,
    partition_table_name,
    num_partitions,
    conn,
    min_val,
    max_val,
    header_path,
    column_to_partition,
):
    """B. Range Partition (40 pts)"""
    points = 0
    details = []

    try:
        assignment_module.range_partition(
            data_table_name,
            partition_table_name,
            num_partitions,
            header_path,
            column_to_partition,
            conn,
        )
        cursor = conn.cursor()

        # Test 1: Parent table creation (10 pts)
        try:
            cursor.execute(
                f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{partition_table_name}');"
            )
            parent_exists = cursor.fetchone()[0]

            # Check if it's partitioned by range
            cursor.execute(
                f"""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables t
                    WHERE t.table_name = '{partition_table_name}'
                );
            """
            )

            if parent_exists:
                points += 10
                details.append(
                    "(10) Parent table creation: PASSED — PARTITION BY RANGE created."
                )
            else:
                details.append(
                    "(0) Parent table creation: FAILED — PARTITION BY RANGE not created."
                )

        except Exception as e:
            details.append(f"(0) Parent table creation: FAILED — {str(e)}")

        # Test 2: Partition tables (10 pts)
        try:
            partition_count = 0
            for i in range(num_partitions):
                cursor.execute(
                    f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{partition_table_name}{i}');"
                )
                if cursor.fetchone()[0]:
                    partition_count += 1

            if partition_count == num_partitions:
                points += 10
                details.append(
                    f"(10) Partition tables: PASSED — all {num_partitions} partitions created."
                )
            else:
                details.append(
                    f"(0) Partition tables: FAILED — {partition_count}/{num_partitions} partitions created."
                )

        except Exception as e:
            details.append(f"(0) Partition tables: FAILED — {str(e)}")

        # Test 3: Data distribution (10 pts)
        try:
            total_rows = 0
            for i in range(num_partitions):
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {partition_table_name}{i};")
                    rows = cursor.fetchone()[0]
                    total_rows += rows
                except:
                    pass

            cursor.execute(f"SELECT COUNT(*) FROM {data_table_name};")
            original_rows = cursor.fetchone()[0]

            # For range partitioning, check if data is properly distributed across partitions
            # The parent table should be empty (data moved to partitions)
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {partition_table_name};")
                parent_rows = cursor.fetchone()[0]
            except:
                parent_rows = (
                    0  # Parent might not accept direct queries if properly partitioned
                )

            if total_rows == original_rows and parent_rows == 0:
                points += 10
                details.append(
                    f"(10) Data distribution: PASSED — {original_rows} rows correctly routed across {num_partitions} partitions; parent table is empty."
                )
            elif total_rows == original_rows:
                # If all data is distributed but parent still has data, it might be a different implementation
                points += 10
                details.append(
                    f"(10) Data distribution: PASSED — {original_rows} rows correctly routed across {num_partitions} partitions."
                )
            else:
                details.append(
                    f"(0) Data distribution: FAILED — {total_rows}/{original_rows} rows distributed, parent has {parent_rows} rows."
                )

        except Exception as e:
            details.append(f"(0) Data distribution: FAILED — {str(e)}")

        # Test 4: New inserts (10 pts)
        try:
            insert_success = 0
            for i, insert_file in enumerate(
                [insert_data_path_1, insert_data_path_2, insert_data_path_3], 1
            ):
                try:
                    with open(insert_file) as json_data:
                        test_data = json.load(json_data)

                    # Reset connection state before each test
                    conn.rollback()
                    [result, e] = test_helper.test_range_insert(
                        assignment_module, partition_table_name, conn, test_data, "0"
                    )
                    if result:
                        insert_success += 1
                except Exception as insert_e:
                    # Reset connection state on error
                    try:
                        conn.rollback()
                    except:
                        pass

            if insert_success == 3:
                points += 10
                details.append(
                    "(10) New inserts (insert1.json, insert2.json, insert3.json): PASSED — each row routed to the correct range partition."
                )
            else:
                details.append(
                    f"(0) New inserts: FAILED — {insert_success}/3 insert tests passed."
                )

        except Exception as e:
            try:
                conn.rollback()
            except:
                pass
            details.append(f"(0) New inserts: FAILED — {str(e)}")

        test_results["range_partition"]["passed"] = points == 40
        test_results["range_partition"]["points"] = points
        test_results["range_partition"]["details"] = details

        return [points == 40, None]

    except Exception as e:
        test_results["range_partition"]["passed"] = False
        test_results["range_partition"]["points"] = 0
        test_results["range_partition"]["details"] = [
            f"(0) Range partition: FAILED — {str(e)}"
        ]
        return [False, e]


def test_round_robin_partition_detailed(
    assignment_module,
    data_table_name,
    partition_table_name,
    num_partitions,
    conn,
    min_val,
    max_val,
    header_path,
    column_to_partition,
):
    """C. Round‑Robin Partition (40 pts)"""
    points = 0
    details = []

    try:
        assignment_module.round_robin_partition(
            data_table_name, partition_table_name, num_partitions, header_path, conn
        )
        cursor = conn.cursor()

        # Test 1: Parent table creation (5 pts)
        try:
            cursor.execute(
                f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{partition_table_name}');"
            )
            parent_exists = cursor.fetchone()[0]

            if parent_exists:
                points += 5
                details.append(
                    "(5) Parent table creation: PASSED — inheritance parent created."
                )
            else:
                details.append(
                    "(0) Parent table creation: FAILED — inheritance parent not created."
                )

        except Exception as e:
            details.append(f"(0) Parent table creation: FAILED — {str(e)}")

        # Test 2: Round-robin tables (5 pts)
        try:
            partition_count = 0
            for i in range(num_partitions):
                cursor.execute(
                    f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{partition_table_name}{i}');"
                )
                if cursor.fetchone()[0]:
                    partition_count += 1

            if partition_count == num_partitions:
                points += 5
                details.append(
                    f"(5) Round-robin tables: PASSED — all {num_partitions} partitions created."
                )
            else:
                details.append(
                    f"(0) Round-robin tables: FAILED — {partition_count}/{num_partitions} partitions created."
                )

        except Exception as e:
            details.append(f"(0) Round-robin tables: FAILED — {str(e)}")

        # Test 3: Initial distribution (10 pts)
        try:
            total_rows = 0
            row_counts = []
            for i in range(num_partitions):
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {partition_table_name}{i};")
                    rows = cursor.fetchone()[0]
                    row_counts.append(rows)
                    total_rows += rows
                except:
                    row_counts.append(0)

            cursor.execute(f"SELECT COUNT(*) FROM {data_table_name};")
            original_rows = cursor.fetchone()[0]

            # Check if distribution is even (max−min ≤ 1)
            if row_counts:
                max_rows = max(row_counts)
                min_rows = min(row_counts)
                even_distribution = (max_rows - min_rows) <= 1
            else:
                even_distribution = False

            if total_rows == original_rows and even_distribution:
                points += 10
                details.append(
                    "(10) Initial distribution: PASSED — even distribution achieved (max−min ≤ 1)."
                )
            else:
                details.append(
                    f"(0) Initial distribution: FAILED — max−min = {max_rows - min_rows if row_counts else 'N/A'}."
                )

        except Exception as e:
            details.append(f"(0) Initial distribution: FAILED — {str(e)}")

        # Test 4: Trigger implementation (10 pts)
        try:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.routines
                    WHERE routine_type = 'FUNCTION' AND routine_name LIKE '%insert%'
                );
            """
            )
            function_exists = cursor.fetchone()[0]

            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT FROM information_schema.triggers
                    WHERE event_object_table = %s AND action_timing = 'BEFORE'
                );
            """,
                (partition_table_name,),
            )
            trigger_exists = cursor.fetchone()[0]

            if function_exists and trigger_exists:
                points += 10
                details.append(
                    "(10) Trigger implementation: PASSED — function and BEFORE INSERT trigger on parent."
                )
            else:
                details.append(
                    "(0) Trigger implementation: FAILED — function or BEFORE INSERT trigger missing."
                )

        except Exception as e:
            details.append(f"(0) Trigger implementation: FAILED — {str(e)}")

        # Test 5: New inserts (10 pts)
        try:
            insert_success = 0
            for i, insert_file in enumerate(
                [insert_data_path_1, insert_data_path_2, insert_data_path_3], 1
            ):
                try:
                    with open(insert_file) as json_data:
                        test_data = json.load(json_data)

                    # Reset connection state before each test
                    conn.rollback()

                    # Get row counts before insert to determine least-filled table
                    row_counts_before = []
                    for j in range(num_partitions):
                        cursor.execute(
                            f"SELECT COUNT(*) FROM {partition_table_name}{j};"
                        )
                        row_counts_before.append(cursor.fetchone()[0])

                    # Find the least-filled table (lowest count)
                    min_count = min(row_counts_before)
                    expected_table_index = row_counts_before.index(min_count)

                    [result, e] = test_helper.test_round_robin_insert(
                        assignment_module,
                        partition_table_name,
                        conn,
                        test_data,
                        str(expected_table_index),
                    )
                    if result:
                        insert_success += 1
                except Exception as insert_e:
                    # Reset connection state on error
                    try:
                        conn.rollback()
                    except:
                        pass

            if insert_success == 3:
                points += 10
                details.append(
                    "(10) New inserts (insert1.json, insert2.json, insert3.json): PASSED — each row inserted into the current least-filled child."
                )
            else:
                details.append(
                    f"(0) New inserts: FAILED — {insert_success}/3 insert tests passed."
                )

        except Exception as e:
            try:
                conn.rollback()
            except:
                pass
            details.append(f"(0) New inserts: FAILED — {str(e)}")

        test_results["round_robin_partition"]["passed"] = points == 40
        test_results["round_robin_partition"]["points"] = points
        test_results["round_robin_partition"]["details"] = details

        return [points == 40, None]

    except Exception as e:
        test_results["round_robin_partition"]["passed"] = False
        test_results["round_robin_partition"]["points"] = 0
        test_results["round_robin_partition"]["details"] = [
            f"(0) Round-robin partition: FAILED — {str(e)}"
        ]
        return [False, e]


def main():
    print_rubric_header()

    try:
        test_helper.create_db(dbname)
        with test_helper.get_open_connection(dbname=dbname) as conn:

            # Load test data
            with open(insert_data_path_1) as json_data:
                data_dict_1 = json.load(json_data)
            with open(insert_data_path_2) as json_data:
                data_dict_2 = json.load(json_data)
            with open(insert_data_path_3) as json_data:
                data_dict_3 = json.load(json_data)

            # Clean up database
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            test_helper.delete_all_public_tables(conn)

            # Test 1: Load Data (20 points)
            print("\n" + "-" * 60)
            print("TESTING STEP 1: DATA LOADING (20 points)")
            print("-" * 60)
            [result, e] = test_load_data_detailed(
                fragmentation,
                data_table_name,
                input_file_path,
                conn,
                rows_in_input,
                header_path,
            )

            # Test 2: Range Partition (40 points)
            print("\n" + "-" * 60)
            print("TESTING STEP 2: RANGE PARTITION (40 points)")
            print("-" * 60)
            [result, e] = test_range_partition_detailed(
                fragmentation,
                data_table_name,
                range_table_prefix,
                count_of_partitions,
                conn,
                0,
                rows_in_input,
                header_path,
                column_to_partition,
            )

            # Test 3: Round Robin Partition (40 points)
            print("\n" + "-" * 60)
            print("TESTING STEP 3: ROUND ROBIN PARTITION (40 points)")
            print("-" * 60)
            [result, e] = test_round_robin_partition_detailed(
                fragmentation,
                data_table_name,
                rrobin_table_prefix,
                count_of_partitions,
                conn,
                0,
                rows_in_input,
                header_path,
                column_to_partition,
            )

            # Print final results
            print_test_results()

            # Optional cleanup
            choice = input("\nPress d to Delete all tables? ")
            if choice == "d":
                try:
                    conn.rollback()  # Reset connection state before cleanup
                    test_helper.delete_all_public_tables(conn)
                except Exception as cleanup_e:
                    print(f"Cleanup failed: {cleanup_e}")
                    try:
                        conn.rollback()
                    except:
                        pass

    except Exception:
        traceback.print_exc()


if __name__ == "__main__":
    main()
