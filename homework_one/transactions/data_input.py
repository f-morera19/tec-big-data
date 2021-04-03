"""
NAME
    data_input.py

DESCRIPTION   
    All logic related to reading external CSV files 
    into DataFrames.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

#   Reads the source csv data into a Spark dataframe and returns it.
def readCsvIntoDataframe(source, csv_schema, csv_header=False):
    """
    Params:
        source: String. Path of source csv file to read.
        csv_schema: StructType. Data schema.
        csv_header: Boolean. Set true if file inlcudes header. False
                    by default.
    """
    validateInput(source, csv_schema)

    dataframe = spark.read.csv(source,
                           schema=csv_schema,
                           header=csv_header)

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