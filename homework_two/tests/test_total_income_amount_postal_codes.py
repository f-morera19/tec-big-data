"""
NAME
    test_total_income.py

DESCRIPTION   
    Unit tests for data input functionality.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021

"""

from src.data_processing import *
from schemas.main_schemas import *

# Test the Union of source data to get all postal 
# codes metadata (i.e. total income and total amount of
# travels made).
def test_get_union_source_data_variousOriginPostalCodes(spark_session):

    rides_data = [
        (48937, 30979, 30728, 17.94, 100.0),
        (48933, 30979, 27839, 29.94, 322.1)
        ]

    rides_df = spark_session.createDataFrame(
        rides_data,
        ride_schema)

    # Generated DataFrame by system's logic.
    actual_df = get_union_source_data(rides_df)

    # Expected result.
    expected_df = spark_session.createDataFrame(
        [
            (27839, 9643.67, 1, 'DESTINO'),
            (30728, 1794.0, 1, 'DESTINO'),
            (30979, 11437.67, 2, 'ORIGEN')
        ],
        postal_code_metadata_schema)
    
    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()

# Test the Union of source data to get all postal 
# codes metadata (i.e. total income and total amount of
# travels made). Substitues null double values by zero (0).
def test_get_union_source_data_variousOriginPostalCodes_nullValues(spark_session):

    rides_data = [
        (48937, 30979, 30728, 17.94, None),
        (48933, 30979, 27839, 29.94, 322.1)
        ]

    rides_df = spark_session.createDataFrame(
        rides_data,
        ride_schema)

    # Generated DataFrame by system's logic.
    actual_df = get_union_source_data(rides_df)

    # Expected result.
    expected_df = spark_session.createDataFrame(
        [
            (27839, 9643.67, 1, 'DESTINO'),
            (30728, 0.0, 1, 'DESTINO'),
            (30979, 9643.67, 2, 'ORIGEN')
        ],
        postal_code_metadata_schema)
    
    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()

# Test the Union of source data to get all postal 
# codes metadata (i.e. total income and total amount of
# travels made).Drops rows with null posta codes when
# grouping by that specific posta code type.
def test_get_union_source_data_variousOriginPostalCodes_nullPostalCodes(spark_session):

    rides_data = [
        (12345, 48937, None, 17.94, 100.0),
        (48933, None, 27839, 29.94, 322.1)
        ]

    rides_df = spark_session.createDataFrame(
        rides_data,
        ride_schema)

    # Generated DataFrame by system's logic.
    actual_df = get_union_source_data(rides_df)

    # Expected result.
    expected_df = spark_session.createDataFrame(
        [
            (27839, 9643.67, 1, 'DESTINO'),
            (48937, 1794.0, 1, 'ORIGEN')
        ],
        postal_code_metadata_schema)
    
    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()

# Test that input data is correctly formated to only include
# total amount that a postal code is repeated whether as 
# destination or origin code.
def test_get_total_amount_formated_df(spark_session):
    postal_codes_data = [
        (27839, 9643.67, 1, 'DESTINO'),
        (48937, 1794.0, 1, 'ORIGEN')
        ]

    postal_codes_df = spark_session.createDataFrame(
        postal_codes_data,
        postal_code_metadata_schema)

    # Generated DataFrame by system's logic.
    actual_df = get_total_amount_formated_df(postal_codes_df)

    # Expected result.
    expected_df = spark_session.createDataFrame(
        [
            (27839, 'DESTINO', 1),
            (48937, 'ORIGEN', 1)
        ],
        postal_code_amount_schema)
    
    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()

# Test that input data is correctly formated to only include
# total income that a postal code is repeated whether as 
# destination or origin code.
def test_get_total_income_formated_df(spark_session):
    postal_codes_data = [
        (27839, 9643.67, 1, 'DESTINO'),
        (48937, 1794.0, 1, 'ORIGEN')
        ]

    postal_codes_df = spark_session.createDataFrame(
        postal_codes_data,
        postal_code_metadata_schema)

    # Generated DataFrame by system's logic.
    actual_df = get_total_income_formated_df(postal_codes_df)

    # Expected result.
    expected_df = spark_session.createDataFrame(
        [
            (27839, 'DESTINO', 9643.67),
            (48937, 'ORIGEN', 1794.0)
        ],
        postal_code_income_schema)
    
    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()