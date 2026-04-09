# Import required libraries
import psycopg2
import traceback
import test_helper
import query_processing


dbname = "fragmentation"
count_of_partitions = 5

# Table nomenclature
data_table_name = "subreddits"
column_to_partition = "created_utc"
range_table_prefix = 'range_part'
rrobin_table_prefix = 'rrobin_part'

# Data files
input_file_path = './subreddits.csv'
header_path = "./headers.json"

# Misc
rows_in_input = 914067

# Test results tracking
test_results = {
    'point_query': {'passed': 0, 'failed': 0, 'points': 0, 'max_points': 50, 'details': []},
    'range_query': {'passed': 0, 'failed': 0, 'points': 0, 'max_points': 50, 'details': []}
}

def print_rubric_header():
    print("=" * 80)
    print("ASSIGNMENT: QUERY PROCESSING - GRADING RUBRIC")
    print("=" * 80)
    print("Total Points: 100")
    print("Step 1: Point Query (50 points)")
    print("  - Range Partitioned Table Tests (25 points)")
    print("  - Round-Robin Partitioned Table Tests (25 points)")
    print("Step 2: Range Query (50 points)")
    print("  - Range Partitioned Table Tests (25 points)")
    print("  - Round-Robin Partitioned Table Tests (25 points)")
    print("=" * 80)

def print_test_results():
    print("\n" + "=" * 80)
    print("FINAL GRADING RESULTS")
    print("=" * 80)

    total_points = 0
    for test_name, result in test_results.items():
        print(f"\n{test_name.replace('_', ' ').title()}:")
        print(f"  Passed: {result['passed']}/{result['passed'] + result['failed']} tests")
        print(f"  Points: {result['points']}/{result['max_points']}")
        if result['details']:
            print("  Details:")
            for detail in result['details']:
                print(f"    {detail}")
        total_points += result['points']

    print(f"\nTOTAL SCORE: {total_points}/100 points")
    print("=" * 80)

def test_output_print(test_name, result, points, test_type):
    """Print test results and track scores"""
    if result[0]:
        print(f"\t✓ {test_name}: PASSED ({points} points)")
        test_results[test_type]['passed'] += 1
        test_results[test_type]['points'] += points
        test_results[test_type]['details'].append(f"✓ {test_name}: PASSED")
    else:
        print(f"\t✗ {test_name}: FAILED (0 points) - {result[1]}")
        test_results[test_type]['failed'] += 1
        test_results[test_type]['details'].append(f"✗ {test_name}: FAILED - {result[1]}")

def main():

    # Note: This assignment assumes that the fragmentation database already exists
    # with the partitioned tables created from the previous assignment.
    # The database should contain:
    # - subreddits table (original data)
    # - range_part (range partitioned table)
    # - rrobin_part (round-robin partitioned table)

    print_rubric_header()
    print("\n------------------------------Starting Tests----------------------------------")
    print("+ Using existing fragmentation database with partitioned tables\n")

    # Clean up any previous test result tables
    print("+ Cleaning up previous test results...")
    with test_helper.get_open_connection(dbname=dbname) as conn:
        cursor = conn.cursor()
        # Drop numbered test tables
        for i in range(1, 25):  # Drop Test1 through Test24
            try:
                cursor.execute(f"DROP TABLE IF EXISTS Test{i} CASCADE;")
            except:
                pass
        # Drop named test tables
        for suffix in ['_range', '_rrobin']:
            for i in range(1, 25):
                try:
                    cursor.execute(f"DROP TABLE IF EXISTS Test{i}{suffix} CASCADE;")
                except:
                    pass
        conn.commit()
        cursor.close()
    print("+ Cleanup complete\n")

    # Testing phase
    try:
        # ===================================================================
        # STEP 1: POINT QUERY TESTS (50 points total)
        # ===================================================================
        print("=" * 80)
        print("STEP 1: POINT QUERY TESTS (50 points)")
        print("=" * 80)

        with test_helper.get_open_connection(dbname=dbname) as conn:

            # Point Query on Range Partitioned Table (25 points)
            print("\n--- Point Query on Range Partitioned Table (25 points) ---")

            # Test 1: Basic point query (5 points)
            print("\n[Test 1] Basic point query with existing UTC value")
            try:
                query_processing.point_query(range_table_prefix, 1201212327, "Test1", conn)
                result = test_helper.test_point_query("test_ops/expected_test1.csv", "Test1", conn)
                test_output_print("Test 1 - Basic point query", result, 5, 'point_query')
            except Exception as e:
                print(f"\t✗ Test 1: FAILED (0 points) - {str(e)}")
                test_results['point_query']['failed'] += 1
                test_results['point_query']['details'].append(f"✗ Test 1: FAILED - {str(e)}")

            # Test 2: Point query with different UTC value (5 points)
            print("\n[Test 2] Point query with different UTC value")
            try:
                query_processing.point_query(range_table_prefix, 1323898614, "Test2", conn)
                result = test_helper.test_point_query("test_ops/expected_test2.csv", "Test2", conn)
                test_output_print("Test 2 - Different UTC value", result, 5, 'point_query')
            except Exception as e:
                print(f"\t✗ Test 2: FAILED (0 points) - {str(e)}")
                test_results['point_query']['failed'] += 1
                test_results['point_query']['details'].append(f"✗ Test 2: FAILED - {str(e)}")

            # Test 3: Point query creates table successfully (5 points)
            print("\n[Test 3] Verify point query creates table successfully")
            try:
                query_processing.point_query(range_table_prefix, 1201212327, "Test3", conn)
                cursor = conn.cursor()
                cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'test3')")
                table_exists = cursor.fetchone()[0]

                if table_exists:
                    print(f"\t✓ Test 3: PASSED (5 points) - Table created successfully")
                    test_results['point_query']['passed'] += 1
                    test_results['point_query']['points'] += 5
                    test_results['point_query']['details'].append(f"✓ Test 3: PASSED")
                else:
                    print(f"\t✗ Test 3: FAILED (0 points) - Table not created")
                    test_results['point_query']['failed'] += 1
                    test_results['point_query']['details'].append(f"✗ Test 3: FAILED - Table not created")
            except Exception as e:
                print(f"\t✗ Test 3: FAILED (0 points) - {str(e)}")
                test_results['point_query']['failed'] += 1
                test_results['point_query']['details'].append(f"✗ Test 3: FAILED - {str(e)}")

            # Test 4: Verify results are stored in correct table (5 points)
            print("\n[Test 4] Verify results are stored in correct table format")
            try:
                query_processing.point_query(range_table_prefix, 1201233921, "Test4", conn)
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM Test4")
                count = cursor.fetchone()[0]
                cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'test4'")
                columns = [row[0] for row in cursor.fetchall()]
                if count > 0 and len(columns) > 0:
                    print(f"\t✓ Test 4: PASSED (5 points) - Table created with {count} rows and {len(columns)} columns")
                    test_results['point_query']['passed'] += 1
                    test_results['point_query']['points'] += 5
                    test_results['point_query']['details'].append(f"✓ Test 4: PASSED")
                else:
                    print(f"\t✗ Test 4: FAILED (0 points) - Invalid table structure")
                    test_results['point_query']['failed'] += 1
                    test_results['point_query']['details'].append(f"✗ Test 4: FAILED - Invalid table structure")
            except Exception as e:
                print(f"\t✗ Test 4: FAILED (0 points) - {str(e)}")
                test_results['point_query']['failed'] += 1
                test_results['point_query']['details'].append(f"✗ Test 4: FAILED - {str(e)}")

            # Test 5: Verify parent partition table is used (5 points)
            print("\n[Test 5] Verify parent partition table is used correctly")
            try:
                # This test verifies that the query works with the parent table
                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM {range_table_prefix} WHERE created_utc = 1201327674")
                expected_count = cursor.fetchone()[0]

                query_processing.point_query(range_table_prefix, 1201327674, "Test5", conn)
                cursor.execute("SELECT COUNT(*) FROM Test5")
                actual_count = cursor.fetchone()[0]

                if expected_count == actual_count:
                    print(f"\t✓ Test 5: PASSED (5 points) - Parent partition table used correctly")
                    test_results['point_query']['passed'] += 1
                    test_results['point_query']['points'] += 5
                    test_results['point_query']['details'].append(f"✓ Test 5: PASSED")
                else:
                    print(f"\t✗ Test 5: FAILED (0 points) - Row count mismatch: expected {expected_count}, got {actual_count}")
                    test_results['point_query']['failed'] += 1
                    test_results['point_query']['details'].append(f"✗ Test 5: FAILED - Row count mismatch")
            except Exception as e:
                print(f"\t✗ Test 5: FAILED (0 points) - {str(e)}")
                test_results['point_query']['failed'] += 1
                test_results['point_query']['details'].append(f"✗ Test 5: FAILED - {str(e)}")

            # Point Query on Round-Robin Partitioned Table (25 points)
            print("\n--- Point Query on Round-Robin Partitioned Table (25 points) ---")

            # Test 6: Basic point query on round-robin (5 points)
            print("\n[Test 6] Basic point query on round-robin partitioned table")
            try:
                query_processing.point_query(rrobin_table_prefix, 1201233921, "Test6", conn)
                result = test_helper.test_point_query("test_ops/expected_test3.csv", "Test6", conn)
                test_output_print("Test 6 - Round-robin basic", result, 5, 'point_query')
            except Exception as e:
                print(f"\t✗ Test 6: FAILED (0 points) - {str(e)}")
                test_results['point_query']['failed'] += 1
                test_results['point_query']['details'].append(f"✗ Test 6: FAILED - {str(e)}")

            # Test 7: Different UTC value on round-robin (5 points)
            print("\n[Test 7] Point query with different UTC on round-robin")
            try:
                query_processing.point_query(rrobin_table_prefix, 1201327674, "Test7", conn)
                result = test_helper.test_point_query("test_ops/expected_test4.csv", "Test7", conn)
                test_output_print("Test 7 - Round-robin different UTC", result, 5, 'point_query')
            except Exception as e:
                print(f"\t✗ Test 7: FAILED (0 points) - {str(e)}")
                test_results['point_query']['failed'] += 1
                test_results['point_query']['details'].append(f"✗ Test 7: FAILED - {str(e)}")

            # Test 8: Point query creates table on round-robin (5 points)
            print("\n[Test 8] Verify point query creates table on round-robin")
            try:
                query_processing.point_query(rrobin_table_prefix, 1201233921, "Test8", conn)
                cursor = conn.cursor()
                cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'test8')")
                table_exists = cursor.fetchone()[0]

                if table_exists:
                    print(f"\t✓ Test 8: PASSED (5 points) - Table created successfully on round-robin")
                    test_results['point_query']['passed'] += 1
                    test_results['point_query']['points'] += 5
                    test_results['point_query']['details'].append(f"✓ Test 8: PASSED")
                else:
                    print(f"\t✗ Test 8: FAILED (0 points) - Table not created")
                    test_results['point_query']['failed'] += 1
                    test_results['point_query']['details'].append(f"✗ Test 8: FAILED - Table not created")
            except Exception as e:
                print(f"\t✗ Test 8: FAILED (0 points) - {str(e)}")
                test_results['point_query']['failed'] += 1
                test_results['point_query']['details'].append(f"✗ Test 8: FAILED - {str(e)}")

            # Test 9: Verify consistency between range and round-robin (5 points)
            print("\n[Test 9] Verify consistency between range and round-robin results")
            try:
                test_utc = 1323898614
                query_processing.point_query(range_table_prefix, test_utc, "Test9_range", conn)
                query_processing.point_query(rrobin_table_prefix, test_utc, "Test9_rrobin", conn)

                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM Test9_range")
                range_count = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM Test9_rrobin")
                rrobin_count = cursor.fetchone()[0]

                if range_count == rrobin_count:
                    print(f"\t✓ Test 9: PASSED (5 points) - Consistent results between partitioning methods")
                    test_results['point_query']['passed'] += 1
                    test_results['point_query']['points'] += 5
                    test_results['point_query']['details'].append(f"✓ Test 9: PASSED")
                else:
                    print(f"\t✗ Test 9: FAILED (0 points) - Inconsistent results: range={range_count}, rrobin={rrobin_count}")
                    test_results['point_query']['failed'] += 1
                    test_results['point_query']['details'].append(f"✗ Test 9: FAILED - Inconsistent results")
            except Exception as e:
                print(f"\t✗ Test 9: FAILED (0 points) - {str(e)}")
                test_results['point_query']['failed'] += 1
                test_results['point_query']['details'].append(f"✗ Test 9: FAILED - {str(e)}")

            # Test 10: Verify no data in save table before query (5 points)
            print("\n[Test 10] Verify query creates new table correctly")
            try:
                # Drop table if exists
                cursor = conn.cursor()
                cursor.execute("DROP TABLE IF EXISTS Test10 CASCADE")
                conn.commit()

                # Run query
                query_processing.point_query(rrobin_table_prefix, 1201212327, "Test10", conn)

                # Verify table exists and has data
                cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'test10')")
                table_exists = cursor.fetchone()[0]

                if table_exists:
                    print(f"\t✓ Test 10: PASSED (5 points) - New table created successfully")
                    test_results['point_query']['passed'] += 1
                    test_results['point_query']['points'] += 5
                    test_results['point_query']['details'].append(f"✓ Test 10: PASSED")
                else:
                    print(f"\t✗ Test 10: FAILED (0 points) - Table not created")
                    test_results['point_query']['failed'] += 1
                    test_results['point_query']['details'].append(f"✗ Test 10: FAILED - Table not created")
            except Exception as e:
                print(f"\t✗ Test 10: FAILED (0 points) - {str(e)}")
                test_results['point_query']['failed'] += 1
                test_results['point_query']['details'].append(f"✗ Test 10: FAILED - {str(e)}")

        # ===================================================================
        # STEP 2: RANGE QUERY TESTS (50 points total)
        # ===================================================================
        print("\n" + "=" * 80)
        print("STEP 2: RANGE QUERY TESTS (50 points)")
        print("=" * 80)

        with test_helper.get_open_connection(dbname=dbname) as conn:

            # Range Query on Range Partitioned Table (25 points)
            print("\n--- Range Query on Range Partitioned Table (25 points) ---")

            # Test 11: Basic range query (5 points)
            print("\n[Test 11] Basic range query with valid range")
            try:
                query_processing.range_query(range_table_prefix, 1201212327, 1201233921, "Test11", conn)
                result = test_helper.test_range_query("test_ops/expected_test5.csv", "Test11", conn)
                test_output_print("Test 11 - Basic range query", result, 5, 'range_query')
            except Exception as e:
                print(f"\t✗ Test 11: FAILED (0 points) - {str(e)}")
                test_results['range_query']['failed'] += 1
                test_results['range_query']['details'].append(f"✗ Test 11: FAILED - {str(e)}")

            # Test 12: Large range query (5 points)
            print("\n[Test 12] Range query with larger range")
            try:
                query_processing.range_query(range_table_prefix, 1201233921, 1213187703, "Test12", conn)
                result = test_helper.test_range_query("test_ops/expected_test6.csv", "Test12", conn)
                test_output_print("Test 12 - Large range query", result, 5, 'range_query')
            except Exception as e:
                print(f"\t✗ Test 12: FAILED (0 points) - {str(e)}")
                test_results['range_query']['failed'] += 1
                test_results['range_query']['details'].append(f"✗ Test 12: FAILED - {str(e)}")

            # Test 13: Small range query (5 points)
            print("\n[Test 13] Range query with small range")
            try:
                query_processing.range_query(range_table_prefix, 1201212321, 1201212327, "Test13", conn)
                result = test_helper.test_range_query("test_ops/expected_test8.csv", "Test13", conn)
                test_output_print("Test 13 - Small range query", result, 5, 'range_query')
            except Exception as e:
                print(f"\t✗ Test 13: FAILED (0 points) - {str(e)}")
                test_results['range_query']['failed'] += 1
                test_results['range_query']['details'].append(f"✗ Test 13: FAILED - {str(e)}")

            # Test 14: Verify results are in ascending order (5 points)
            print("\n[Test 14] Verify results are sorted in ascending order by created_utc")
            try:
                query_processing.range_query(range_table_prefix, 1201212321, 1201233921, "Test14", conn)
                cursor = conn.cursor()
                cursor.execute("SELECT created_utc FROM Test14")
                utc_values = [row[0] for row in cursor.fetchall()]

                is_sorted = all(utc_values[i] <= utc_values[i+1] for i in range(len(utc_values)-1))

                if is_sorted and len(utc_values) > 0:
                    print(f"\t✓ Test 14: PASSED (5 points) - Results sorted correctly ({len(utc_values)} rows)")
                    test_results['range_query']['passed'] += 1
                    test_results['range_query']['points'] += 5
                    test_results['range_query']['details'].append(f"✓ Test 14: PASSED")
                else:
                    print(f"\t✗ Test 14: FAILED (0 points) - Results not sorted correctly")
                    test_results['range_query']['failed'] += 1
                    test_results['range_query']['details'].append(f"✗ Test 14: FAILED - Results not sorted")
            except Exception as e:
                print(f"\t✗ Test 14: FAILED (0 points) - {str(e)}")
                test_results['range_query']['failed'] += 1
                test_results['range_query']['details'].append(f"✗ Test 14: FAILED - {str(e)}")

            # Test 15: Verify exclusive/inclusive bounds (UTCMinValue, UTCMaxValue] (5 points)
            print("\n[Test 15] Verify range query uses correct bounds (UTCMinValue, UTCMaxValue]")
            try:
                min_val = 1201212321
                max_val = 1201212327
                query_processing.range_query(range_table_prefix, min_val, max_val, "Test15", conn)

                cursor = conn.cursor()
                cursor.execute(f"SELECT COUNT(*) FROM Test15 WHERE created_utc = {min_val}")
                has_min = cursor.fetchone()[0]
                cursor.execute(f"SELECT COUNT(*) FROM Test15 WHERE created_utc = {max_val}")
                has_max = cursor.fetchone()[0]
                cursor.execute(f"SELECT COUNT(*) FROM Test15 WHERE created_utc <= {min_val} OR created_utc > {max_val}")
                has_invalid = cursor.fetchone()[0]

                if has_min == 0 and has_max > 0 and has_invalid == 0:
                    print(f"\t✓ Test 15: PASSED (5 points) - Correct bounds: excludes min, includes max")
                    test_results['range_query']['passed'] += 1
                    test_results['range_query']['points'] += 5
                    test_results['range_query']['details'].append(f"✓ Test 15: PASSED")
                else:
                    print(f"\t✗ Test 15: FAILED (0 points) - Incorrect bounds (min_included={has_min}, max_included={has_max}, invalid={has_invalid})")
                    test_results['range_query']['failed'] += 1
                    test_results['range_query']['details'].append(f"✗ Test 15: FAILED - Incorrect bounds")
            except Exception as e:
                print(f"\t✗ Test 15: FAILED (0 points) - {str(e)}")
                test_results['range_query']['failed'] += 1
                test_results['range_query']['details'].append(f"✗ Test 15: FAILED - {str(e)}")

            # Range Query on Round-Robin Partitioned Table (25 points)
            print("\n--- Range Query on Round-Robin Partitioned Table (25 points) ---")

            # Test 16: Basic range query on round-robin (5 points)
            print("\n[Test 16] Basic range query on round-robin partitioned table")
            try:
                query_processing.range_query(rrobin_table_prefix, 1301212327, 1301233921, "Test16", conn)
                result = test_helper.test_range_query("test_ops/expected_test7.csv", "Test16", conn)
                test_output_print("Test 16 - Round-robin basic range", result, 5, 'range_query')
            except Exception as e:
                print(f"\t✗ Test 16: FAILED (0 points) - {str(e)}")
                test_results['range_query']['failed'] += 1
                test_results['range_query']['details'].append(f"✗ Test 16: FAILED - {str(e)}")

            # Test 17: Small range query on round-robin (5 points)
            print("\n[Test 17] Small range query on round-robin")
            try:
                query_processing.range_query(rrobin_table_prefix, 1201212321, 1201212327, "Test17", conn)
                result = test_helper.test_range_query("test_ops/expected_test8.csv", "Test17", conn)
                test_output_print("Test 17 - Round-robin small range", result, 5, 'range_query')
            except Exception as e:
                print(f"\t✗ Test 17: FAILED (0 points) - {str(e)}")
                test_results['range_query']['failed'] += 1
                test_results['range_query']['details'].append(f"✗ Test 17: FAILED - {str(e)}")

            # Test 18: Large range query on round-robin (5 points)
            print("\n[Test 18] Large range query on round-robin")
            try:
                query_processing.range_query(rrobin_table_prefix, 1201212327, 1201233921, "Test18", conn)
                result = test_helper.test_range_query("test_ops/expected_test5.csv", "Test18", conn)
                test_output_print("Test 18 - Round-robin large range", result, 5, 'range_query')
            except Exception as e:
                print(f"\t✗ Test 18: FAILED (0 points) - {str(e)}")
                test_results['range_query']['failed'] += 1
                test_results['range_query']['details'].append(f"✗ Test 18: FAILED - {str(e)}")

            # Test 19: Verify sorting on round-robin (5 points)
            print("\n[Test 19] Verify results are sorted on round-robin table")
            try:
                query_processing.range_query(rrobin_table_prefix, 1201212327, 1213187703, "Test19", conn)
                cursor = conn.cursor()
                cursor.execute("SELECT created_utc FROM Test19")
                utc_values = [row[0] for row in cursor.fetchall()]

                is_sorted = all(utc_values[i] <= utc_values[i+1] for i in range(len(utc_values)-1))

                if is_sorted and len(utc_values) > 0:
                    print(f"\t✓ Test 19: PASSED (5 points) - Results sorted correctly on round-robin")
                    test_results['range_query']['passed'] += 1
                    test_results['range_query']['points'] += 5
                    test_results['range_query']['details'].append(f"✓ Test 19: PASSED")
                else:
                    print(f"\t✗ Test 19: FAILED (0 points) - Results not sorted correctly")
                    test_results['range_query']['failed'] += 1
                    test_results['range_query']['details'].append(f"✗ Test 19: FAILED - Results not sorted")
            except Exception as e:
                print(f"\t✗ Test 19: FAILED (0 points) - {str(e)}")
                test_results['range_query']['failed'] += 1
                test_results['range_query']['details'].append(f"✗ Test 19: FAILED - {str(e)}")

            # Test 20: Verify consistency between range and round-robin partitioning (5 points)
            print("\n[Test 20] Verify consistency between range and round-robin partitioning")
            try:
                min_val = 1201212327
                max_val = 1201233921
                query_processing.range_query(range_table_prefix, min_val, max_val, "Test20_range", conn)
                query_processing.range_query(rrobin_table_prefix, min_val, max_val, "Test20_rrobin", conn)

                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM Test20_range")
                range_count = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(*) FROM Test20_rrobin")
                rrobin_count = cursor.fetchone()[0]

                if range_count == rrobin_count and range_count > 0:
                    print(f"\t✓ Test 20: PASSED (5 points) - Consistent results: {range_count} rows")
                    test_results['range_query']['passed'] += 1
                    test_results['range_query']['points'] += 5
                    test_results['range_query']['details'].append(f"✓ Test 20: PASSED")
                else:
                    print(f"\t✗ Test 20: FAILED (0 points) - Inconsistent results: range={range_count}, rrobin={rrobin_count}")
                    test_results['range_query']['failed'] += 1
                    test_results['range_query']['details'].append(f"✗ Test 20: FAILED - Inconsistent results")
            except Exception as e:
                print(f"\t✗ Test 20: FAILED (0 points) - {str(e)}")
                test_results['range_query']['failed'] += 1
                test_results['range_query']['details'].append(f"✗ Test 20: FAILED - {str(e)}")

        # Print final results
        print_test_results()

    except Exception as e:
        print("\n\nCRITICAL ERROR: ", e)
        traceback.print_exc()
        print_test_results()


if __name__ == '__main__':
    main()
