"""
NAME
    vj_critics_schemas.py

DESCRIPTION   
    Schemas related with Videogame critics data.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *

#   Defined schema for Videogame critics DataFrame.
critics_schema = StructType(
    [
        StructField('name', StringType()),
        StructField('platform', StringType()),
        StructField('developer', StringType()),   
        StructField('publisher', StringType()),
        StructField('genre', StringType()),
        StructField('players', StringType()),
        StructField('esrb_rating', StringType()),
        StructField('attribute', StringType()),
        StructField('release_date', StringType()),
        StructField('link', StringType()),
        StructField('critic_positive', IntegerType()),
        StructField('critic_neutral', IntegerType()),
        StructField('critic_negative', IntegerType()),
        StructField('metascore', IntegerType()),
        StructField('user_positive', IntegerType()),
        StructField('user_neutral', IntegerType()),
        StructField('user_negative', IntegerType()),
        StructField('user_score', StringType())
    ])