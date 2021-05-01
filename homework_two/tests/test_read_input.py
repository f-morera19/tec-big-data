"""
NAME
    test_read_input.py

DESCRIPTION   
    Unit tests for data input functionality.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021

"""

from src.read_input import *
from schemas.main_schemas import *

# Test json as column to regular columns flattening,
# where no identifiers are null.
def test_flatten_jsonColumn_noNullIdentifiers(spark_session):

    rides_data = [
        (48937, [(30979, 49679, 17.94, 361.0)])
    ]

    rides_df = spark_session.createDataFrame(
        rides_data,
        unflatten_ride_schema)

    # Generated DataFrame by system's logic.
    actual_df = flatten_jsonColumn(rides_df)

    # Expected result.
    expected_df = spark_session.createDataFrame(
        [
            (48937, 30979, 49679, 17.94, 361.0)
        ],
        ride_schema)
    
    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()

# Test json as column to regular columns flattening,
# where some identifiers are null, therefore droped.
def test_flatten_jsonColumn_withNullIdentifiers(spark_session):

    rides_data = [
        (48937, [(30979, 49679, 17.94, 361.0)]),
        (None, [(30979, 49679, 17.94, 361.0)])
    ]

    rides_df = spark_session.createDataFrame(
        rides_data,
        unflatten_ride_schema)

    # Generated DataFrame by system's logic.
    actual_df = flatten_jsonColumn(rides_df)

    # Expected result.
    expected_df = spark_session.createDataFrame(
        [
            (48937, 30979, 49679, 17.94, 361.0)
        ],
        ride_schema)
    
    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()
