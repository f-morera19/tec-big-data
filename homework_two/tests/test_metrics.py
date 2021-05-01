"""
NAME
    test_metrics.py

DESCRIPTION   
    Unit tests for matric generation functionality.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021

"""

from src.data_processing import *
from schemas.main_schemas import *

# Test metric for getting the driver id
# with the most traveled distance.
def test_get_most_distance_driver(spark_session):
    
    rides_data = [
        (48937, 30979, 30728, 17.94, 100.0),
        (48933, 30979, 27839, 29.94, 322.1)
    ]

    rides_df = spark_session.createDataFrame(
        rides_data,
        ride_schema)

    # Generated DataFrame by system's logic.
    actual_result = get_most_distance_driver(rides_df)
    
     # Expected result.
    expected_result = 48933
    
    print("Expected Result: ", expected_result)
    print("Actual Result: ", actual_result)

    assert actual_result == expected_result

# Test metric for getting the driver id
# with the most generated income.
def test_get_most_income_driver(spark_session):
    
    rides_data = [
        (48937, 30979, 30728, 52.94, 112.0),
        (48937, 30979, 30728, 22.12, 221.2),
        (48933, 30979, 27839, 29.94, 322.1)
    ]

    rides_df = spark_session.createDataFrame(
        rides_data,
        ride_schema)

    # Generated DataFrame by system's logic.
    actual_result = get_most_income_driver(rides_df)
    
     # Expected result.
    expected_result = 48937
    
    print("Expected Result: ", expected_result)
    print("Actual Result: ", actual_result)

    assert actual_result == expected_result

# Test metric for getting the origin postal 
# code with the most income generated.
def test_get_origin_postal_code_most_income(spark_session):
    
    rides_data = [
        (48937, 23190, 12394, 52.94, 112.0),
        (48937, 30979, 30728, 22.12, 221.2),
        (48933, 12394, 27839, 29.94, 322.1)
    ]

    rides_df = spark_session.createDataFrame(
        rides_data,
        ride_schema)

    # Generated DataFrame by system's logic.
    actual_result = get_origin_postal_code_most_income(rides_df)
    
     # Expected result.
    expected_result = 12394
    
    print("Expected Result: ", expected_result)
    print("Actual Result: ", actual_result)

    assert actual_result == expected_result

# Test metric for getting the dstination postal 
# code with the most income generated.
def test_get_destiny_postal_code_most_income(spark_session):
    
    rides_data = [
        (48937, 23190, 12394, 52.94, 112.0),
        (48937, 30979, 30728, 22.12, 221.2),
        (48933, 12394, 27839, 29.94, 322.1)
    ]

    rides_df = spark_session.createDataFrame(
        rides_data,
        ride_schema)

    # Generated DataFrame by system's logic.
    actual_result = get_destiny_postal_code_most_income(rides_df)
    
     # Expected result.
    expected_result = 27839
    
    print("Expected Result: ", expected_result)
    print("Actual Result: ", actual_result)

    assert actual_result == expected_result

# Test metric for getting the 25th
# percentil monetary value. Sorting by sales amount
# for specifics user ids.
def test_get_percentil_25(spark_session):
    
    rides_data = [
        (48937, 23190, 12394, 52.94, 112.0),
        (48937, 30979, 30728, 22.12, 221.2),
        (48933, 12394, 27839, 29.94, 322.1)
    ]

    rides_df = spark_session.createDataFrame(
        rides_data,
        ride_schema)

    # Generated DataFrame by system's logic.
    actual_result = get_percentil(rides_df, '0.25')
    
     # Expected result.
    expected_result = 9938.31
    
    print("Expected Result: ", expected_result)
    print("Actual Result: ", actual_result)

    assert actual_result == expected_result

# Test metric for getting the 50th
# percentil monetary value. Sorting by sales amount
# for specifics user ids.
def test_get_percentil_50(spark_session):
    
    rides_data = [
        (48937, 23190, 12394, 52.94, 112.0),
        (48937, 30979, 30728, 22.12, 221.2),
        (48933, 12394, 27839, 29.94, 322.1)
    ]

    rides_df = spark_session.createDataFrame(
        rides_data,
        ride_schema)

    # Generated DataFrame by system's logic.
    actual_result = get_percentil(rides_df, '0.50')
    
     # Expected result.
    expected_result = 10232.95
    
    print("Expected Result: ", expected_result)
    print("Actual Result: ", actual_result)

    assert actual_result == expected_result

# Test metric for getting the 75th
# percentil monetary value. Sorting by sales amount
# for specifics user ids.
def test_get_percentil_75(spark_session):
    
    rides_data = [
        (48937, 23190, 12394, 52.94, 112.0),
        (48937, 30979, 30728, 22.12, 221.2),
        (48933, 12394, 27839, 29.94, 322.1)
    ]

    rides_df = spark_session.createDataFrame(
        rides_data,
        ride_schema)

    # Generated DataFrame by system's logic.
    actual_result = get_percentil(rides_df, '0.75')
    
     # Expected result.
    expected_result = 10527.59
    
    print("Expected Result: ", expected_result)
    print("Actual Result: ", actual_result)

    assert actual_result == expected_result