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

DEFAULT_ALL_FILES_PATH = 'resources/*.json'

# Read multiple json files from a folder.
def readJsonFilesFromPath(
    path, 
    showdf=False, 
    showSchema=False):
    """
    Params:
        path (string): Path to read files from.
        showdf (boolean): Show the dataframe in console.
        showSchema (boolean): Show the dataframe schema in console.
    Returns: 
        DataFrame: Read files as DataFrame.
    """

    spark = SparkSession.builder.appName("Read Transactions").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    input_df = None

    if (path is None) or (not path): 
        # Read all JSON files from a folder with rider_schema by default.
        input_df = spark.read.option("multiline","true").json(DEFAULT_ALL_FILES_PATH)
    else:
        # Read explicit files only.
        input_df = spark.read.option("multiline","true").json(path)
        
    if showdf:
        input_df.show()

    if showSchema:
        input_df.printSchema()

    return input_df

# Extract the json properties into their own columns and cast properly.
def flatten_jsonColumn(
    input_df,
    showdf=False,
    showSchema=False):
    """
    Params:
        input_df (DataFrame): data to transform into
        one we can work on.
        showdf (boolean): Show the dataframe in console.
        showSchema (boolean): Show the dataframe schema in console.
    Returns: 
        DataFrame: Properly formatted dataframe.
    """
    
    result_df = input_df \
        .withColumn(
            "identificador",
            F.col("identificador").cast(IntegerType())) \
        .withColumn(
            "data",
            F.explode(F.col("viajes"))) \
        .select(
            F.col("identificador").alias("user_id"),
            F.col("data.codigo_postal_origen").cast(IntegerType()),
            F.col("data.codigo_postal_destino").cast(IntegerType()),
            F.col("data.kilometros").cast(DoubleType()),
            F.col("data.precio_kilometro").cast(DoubleType()))\
        .na.drop(subset=["user_id"])
    
    if showdf:
        result_df.show()

    if showSchema:
        result_df.printSchema()

    return result_df