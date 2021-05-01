"""
NAME
    vj_sales_schemas.py

DESCRIPTION   
    Schemas related with Videogame sales data.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *

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
        StructField('year', IntegerType())
    ])