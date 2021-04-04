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
from transactions.data_aggregation import *
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

print("Union DataFrame.\n")

#   Create union DF from source DTs.
union_df = union(cyclists_df, routes_df, activities_df)
union_df.printSchema()
union_df.show()

print("Aggregations DataFrames.\n")

#   Create aggregation DF by cyclists.
agg_by_cyclists = aggregateByCyclist(union_df)
agg_by_cyclists.show()

#   Create aggregation DF by route.
agg_by_route = aggregateByRoute(union_df)
agg_by_route.show()

#   Create aggregation DF by province.
agg_by_province = aggregateByProvince(union_df)
agg_by_province.show()

#   Create aggregation DF by date.
agg_by_date = aggregateByDate(union_df)
agg_by_date.show()