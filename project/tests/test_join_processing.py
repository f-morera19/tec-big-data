"""
NAME
    test_join_processing.py

DESCRIPTION   
    Unit tests for critics and sales join processing functionality.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021

"""

# Imports.
from src.processing import *
from schemas.schemas import *

# Join cleaned datasets for critics and sales, matching name.
def test_data_join_noNullValues(spark_session):

    # Mock critics data.
    critics_data = [
        ('Quake',9,0,0,94,84,4,1,8.8,'QUAKE',1,0,0,0,1,0,0,1,0,0,1,1)
        ]

    critics_df = spark_session.createDataFrame(
        critics_data,
        formatted_critics_schema)

    # Mock sales data.
    sales_data = [
        ('Quake', 7.7, 9.1, 82.86, 2006, 'QUAKE')
        ]

    sales_df = spark_session.createDataFrame(
        sales_data,
        formatted_sales_schema)

     # Generated DataFrame by system's logic.
    actual_df = data_join(sales_df, critics_df)

    # Expected result.
    expected_df = spark_session.createDataFrame(
        [
            (7.7, 9.1, 82.86, 2006,'QUAKE',9,0,0,94,84,4,1,8.8,1,0,0,0,1,0,0,1,0,0,1,1)
        ],
        formatted_critics_sales_schema)
    
    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()