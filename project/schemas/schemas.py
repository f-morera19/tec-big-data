"""
NAME
    schemas.py

DESCRIPTION   
    Program´s defined schemas.

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
        StructField('c_pos', IntegerType()),
        StructField('c_neu', IntegerType()),
        StructField('c_neg', IntegerType()),
        StructField('metascore', IntegerType()),
        StructField('u_pos', IntegerType()),
        StructField('u_neu', IntegerType()),
        StructField('u_neg', IntegerType()),
        StructField('metacritic_user_score', StringType())
    ])

#   Defined schema for Videogame Sales DataFrame.
sales_schema = StructType(
    [
        StructField('rank', IntegerType()),
        StructField('name', StringType()),
        StructField('genre', StringType()),    
        StructField('esrb_rating', StringType()),
        StructField('platform', StringType()),
        StructField('publisher', StringType()),       
        StructField('developer', StringType()),
        StructField('critic_score', DoubleType()),
        StructField('user_score', DoubleType()),
        StructField('total_shipped', DoubleType()),
        StructField('global_sales', DoubleType()),
        StructField('na_sales', DoubleType()),
        StructField('pal_sales', DoubleType()),
        StructField('jp_sales', DoubleType()),
        StructField('other_sales', DoubleType()),
        StructField('year', StringType())
    ])

formatted_sales_schema = StructType(
    [
        StructField('name', StringType()),
        StructField('critic_score', DoubleType()),    
        StructField('user_score', DoubleType()),
        StructField('total_shipped', DoubleType()),
        StructField('year', IntegerType()), 
        StructField('formatted_name', StringType())
    ])

formatted_critics_schema = StructType(
    [
        StructField('name', StringType()),
        StructField('c_pos', IntegerType()),
        StructField('c_neu', IntegerType()),
        StructField('c_neg', IntegerType()),
        StructField('metascore', IntegerType()),
        StructField('u_pos', IntegerType()),
        StructField('u_neu', IntegerType()),
        StructField('u_neg', IntegerType()),
        StructField('metacritic_user_score', DoubleType()),
        StructField('metacritic_formatted_name', StringType()),
        StructField('isPCPlatform', IntegerType()),
        StructField('isX360Platform', IntegerType()),   
        StructField('isOtherPlatform', IntegerType()),
        StructField('isERating', IntegerType()),
        StructField('isTRating', IntegerType()),
        StructField('isMRating', IntegerType()),
        StructField('isOtherRating', IntegerType()),
        StructField('isActionGenre', IntegerType()),
        StructField('isActionAdventureGenre', IntegerType()),
        StructField('isOtherGenre', IntegerType()),
        StructField('isOnePlayerOnly', IntegerType()),
        StructField('hasOnlineMultiplayer', IntegerType())
    ])