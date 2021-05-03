"""
NAME
    test_vgsales_processing.py

DESCRIPTION   
    Unit tests for vg sales processing functionality.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021

"""

# Imports.
from src.processing import *
from schemas.schemas import *

# Format Sales Dataframe with just Total Shipped value 
# and no Global Sales.
def test_format_sales_dataframe_whenNoNullValues(spark_session):

    sales_data = [
        (1, 'Wii Sports', 'Sports', 'E', 'Wii', 'Nintendo', 'Nintendo EAD',7.7,9.1,82.86,None,None,None,None,None,2006.0)
        ]

    sales_df = spark_session.createDataFrame(
        sales_data,
        sales_schema)

    # Generated DataFrame by system's logic.
    actual_df = format_sales_dataframe(sales_df)

    # Expected result.
    expected_df = spark_session.createDataFrame(
        [
            ('Wii Sports', 7.7, 9.1, 82.86, 2006, 'WIISPORTS')
        ],
        formatted_sales_schema)
    
    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()

# Format Sales Dataframe with just Global Sales value
# and no Total Shipped.
def test_format_sales_dataframe_whenNoTotalShippedButGlobalSales(spark_session):

    sales_data = [
        (1, 'Wii Sports', 'Sports', 'E', 'Wii', 'Nintendo', 'Nintendo EAD',7.7,9.1,None,20.32,6.37,9.85,0.99,3.12,2006.0)
        ]

    sales_df = spark_session.createDataFrame(
        sales_data,
        sales_schema)

    # Generated DataFrame by system's logic.
    actual_df = format_sales_dataframe(sales_df)

    # Expected result.
    expected_df = spark_session.createDataFrame(
        [
            ('Wii Sports', 7.7, 9.1, 20.32, 2006, 'WIISPORTS')
        ],
        formatted_sales_schema)
    
    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()

# Format Sales Dataframe with null name, then drop register.
def test_format_sales_dataframe_whenNoTotalShippedButGlobalSalesAndNullName(spark_session):

    sales_data = [
        (1, None, 'Sports', 'E', 'Wii', 'Nintendo', 'Nintendo EAD',7.7,9.1,None,20.32,6.37,9.85,0.99,3.12,2006.0),
        (2,'Mario', 'Sports', 'E', 'Wii', 'Nintendo', 'Nintendo EAD',7.7,9.1,None,20.32,6.37,9.85,0.99,3.12,2006.0)
        ]

    sales_df = spark_session.createDataFrame(
        sales_data,
        sales_schema)

    # Generated DataFrame by system's logic.
    actual_df = format_sales_dataframe(sales_df)

    # Expected result.
    expected_df = spark_session.createDataFrame(
        [
            ('Mario', 7.7, 9.1, 20.32, 2006, 'MARIO')
        ],
        formatted_sales_schema)
    
    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()

# Format Sales Dataframe with Critic Score, then drop register.
def test_format_sales_dataframe_whenNoTotalShippedButGlobalSalesAndNullCriticScore(spark_session):

    sales_data = [
        (1, 'Wii Sports' , 'Sports', 'E', 'Wii', 'Nintendo', 'Nintendo EAD',7.7,9.1,None,20.32,6.37,9.85,0.99,3.12,2006.0),
        (2, 'Mario', 'Sports', 'E', 'Wii', 'Nintendo', 'Nintendo EAD',None,9.1,None,20.32,6.37,9.85,0.99,3.12,2006.0)
        ]

    sales_df = spark_session.createDataFrame(
        sales_data,
        sales_schema)

    # Generated DataFrame by system's logic.
    actual_df = format_sales_dataframe(sales_df)

    # Expected result.
    expected_df = spark_session.createDataFrame(
        [
            ('Wii Sports', 7.7, 9.1, 20.32, 2006, 'WIISPORTS')
        ],
        formatted_sales_schema)
    
    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()