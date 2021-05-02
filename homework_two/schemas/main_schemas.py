"""
NAME
    main_schemas.py

DESCRIPTION   
    Various dataframes' schemas.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *

unflatten_ride_schema = StructType(
    [
        StructField("identificador", IntegerType()),
        StructField("viajes", ArrayType(
            StructType(
                [
                    StructField("codigo_postal_origen", IntegerType(), True), 
                    StructField("codigo_postal_destino", IntegerType(), True), 
                    StructField("kilometros", DoubleType(), True), 
                    StructField("precio_kilometro", DoubleType(), True)
                ])
        ))  
    ])

ride_schema = StructType(
    [
        StructField("user_id", IntegerType(), True),
        StructField("codigo_postal_origen", IntegerType(), True), 
        StructField("codigo_postal_destino", IntegerType(), True), 
        StructField("kilometros", DoubleType(), True), 
        StructField("precio_kilometro", DoubleType(), True)
    ])

postal_code_metadata_schema = StructType(
    [
        StructField("codigo_postal", IntegerType(), True),
        StructField("total_income", DoubleType(), True), 
        StructField("total_amount", IntegerType(), True),
        StructField("type", StringType(), True),
    ])

postal_code_income_schema = StructType(
    [
        StructField("codigo_postal", IntegerType(), True),
        StructField("type", StringType(), True),
         StructField("total_income", DoubleType(), True)
    ])

postal_code_amount_schema = StructType(
    [
        StructField("codigo_postal", IntegerType(), True),
        StructField("type", StringType(), True),
         StructField("total_amount", IntegerType(), True)
    ])