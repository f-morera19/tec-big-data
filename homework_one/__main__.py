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

import argparse
from pyspark.sql import SparkSession
from helpers.args_setup import *
from transactions.data_input import *
from transactions.data_union import union
from schemas.main_schemas import *

#   Set the input paths.
cyclists_path, activities_path, routes_path = get_args()

#   Read the source Cyclists, Activities and Routes csv files.
cyclists_df = readCsvIntoDataframe(
    source=cyclists_path,
    csv_schema=cyclists_schema)

activities_df = readCsvIntoDataframe(
    source=activities_path,
    csv_schema=activities_schema)

routes_df = readCsvIntoDataframe(
    source=routes_path,
    csv_schema=routes_schema)

cyclists_df.show()
activities_df.show()
routes_df.show()

#   Create union DF from source DTs.
union_df = union(cyclists_df, routes_df, activities_df)
union_df.show()

#   Create the partial aggregation DTs.

#   Create functions that return the top N of cyclists. (low priority)