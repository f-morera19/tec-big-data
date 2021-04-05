"""
NAME
    schema_dict.py

DESCRIPTION   
    Various dataframes' schemas.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, DateType, DoubleType

#   Defined schema for Cyclists DataFrames.
cyclists_schema = StructType(
    [
        StructField('id', IntegerType()),
        StructField('full_name', StringType()),
        StructField('province', StringType())
    ])

#   Defined schema for Routes DataFrames.
routes_schema = StructType(
    [
        StructField('code', IntegerType()),
        StructField('name', StringType()),
        StructField('distance', DoubleType())
    ])

#   Defined schema for Activies DataFrames.
activities_schema = StructType(
    [
        StructField('route_code', IntegerType()),
        StructField('cyclist_id', IntegerType()),
        StructField('date', DateType())
    ])

cyclists_activities_schema = StructType(
    [
        StructField('id', IntegerType()),
        StructField('full_name', StringType()),
        StructField('province', StringType()),
        StructField('route_code', IntegerType()),
        StructField('date', DateType())
    ])

cyclists_activities_routes_schema = StructType(
    [
        StructField('id', IntegerType()),
        StructField('full_name', StringType()),
        StructField('province', StringType()),
        StructField('route_code', IntegerType()),
        StructField('route_name', StringType()),
        StructField('distance', DoubleType()),
        StructField('date', DateType())
    ])
