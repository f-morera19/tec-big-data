"""
NAME
    read_inpuy.py

DESCRIPTION   
    Read input data from csv source files.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021
"""

# Dependencies.
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

RESOURCES_FOLDER_PATH = 'resources/'

#   Reads the source csv data into a Spark dataframe and returns it.
def readCsvIntoDataframe(
    source,
    csv_schema,
    csv_header=False):
    """
    Params:
        source: String. Path of source csv file to read.
        csv_schema: StructType. Data schema.
        csv_header: Boolean. Set true if file inlcudes header. False
                    by default.
    """
    validateInput(source, csv_schema)

    spark = SparkSession \
        .builder \
        .appName("Basic JDBC pipeline") \
        .config("spark.driver.extraClassPath", "postgresql-42.2.14.jar") \
        .config("spark.executor.extraClassPath", "postgresql-42.2.14.jar") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    dataframe = spark.read.csv(
        RESOURCES_FOLDER_PATH + source, 
        schema=csv_schema, 
        header=csv_header)

    print(f"Read a total of: {dataframe.count()} rows for {source}.")

    return dataframe

#   Validate the data source and the schema.
def validateInput(source, csv_schema):
    """
    Params:
        source: String. Path of source csv file to read.
        csv_schema: StructType. Data schema.
    """
    if (not type(source) is str) or (source is None):
        raise TypeError("Data Source must be a valid string in order to read.") 

    if (not type(csv_schema) is StructType) or (csv_schema is None):
        raise TypeError("Schema must be a valid StructType.") 