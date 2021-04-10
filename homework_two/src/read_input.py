"""
NAME
    read_input.py

DESCRIPTION   
    Data input.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import * 
from schemas.diber_schemas import *

# Read multiple json files from a folder.
def readJsonFilesFromPath(path='resources/*.json', show=False):
    """
    Params:
        path (string): Path to read files from.
        show (boolean): Show the dataframe in console.
    Returns: 
        DataFrame: Read files as DataFrame.
    """

    spark = SparkSession.builder.appName("Read Transactions").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Read all JSON files from a folder.
    input_df = spark.read.option("multiline","true") \
      .json(path)

    if show: 
        input_df.show()

    return input_df

# Extract the json properties into their own columns.
def flatten_jsonColumn(input_df, showdf=False, showSchema=False):
    """
    Params:
        input_df (DataFrame): data to transform into
        one we can work on.
        show (boolean): Show the dataframe in console.
        showSchema (boolean): Show the dataframe schema in console.
    Returns: 
        DataFrame: Properly formatted dataframe.
    """

    df_temp = input_df.select(F.col("identificador"),\
         F.explode(F.col("viajes")))

    result_df = df_temp \
        .select(\
            F.col("identificador").alias("user_id"), \
            F.col("col.*"))

    if showdf:
        result_df.show()

    if showSchema:
        result_df.printSchema()

    return result_df

    