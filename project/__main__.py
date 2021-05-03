"""
NAME
    __main__.py

DESCRIPTION   
    Program entry point.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021
"""

# Dependencies.
from pyspark.sql import SparkSession
from src.read_input import *
from src.processing import *
from src.write_data import *
from schemas.schemas import *

SALES_CSV_RESOURCE_PATH = 'vg_sales.csv'
CRITICS_CSV_RESOURCE_PATH = 'vg_metacritic.csv'
SALES_DB_NAME = 'vg_sales'
CRITICS_DB_NAME = 'vg_critics'
RESULTS_DB_NAME = 'vg_critic_sales'

# Read videogame sales data into DataFrame.
original_sales_df = readCsvIntoDataframe(
    source=SALES_CSV_RESOURCE_PATH,
    csv_schema=sales_schema)

# Read videogame critics data into DataFrame.
original_critics_df = readCsvIntoDataframe(
    source=CRITICS_CSV_RESOURCE_PATH,
    csv_schema=critics_schema)

# Fix and clean boths dataframes data formats.
sales_df = format_sales_dataframe(original_sales_df, False)
critics_df = format_critics_dataframe(original_critics_df, False)
critics_df.printSchema()

# Save both dataframes in PostgreSQL before mergin.
save_data_DB(sales_df, SALES_DB_NAME)
save_data_DB(critics_df, CRITICS_DB_NAME)

# Join both dataframes.
result_df = data_join(
    sales_source_df=sales_df,
    critics_source_df=critics_df,
    showdf=True)

# Save in DB the result dataframe after joining the original ones.
save_data_DB(result_df, RESULTS_DB_NAME)