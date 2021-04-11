"""
NAME
    diber_schemas.py

DESCRIPTION   
    Various dataframes' schemas.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import *

# Defined per assignment definition.
ride_schema = StructType(
    [ 
        StructField("identificador", IntegerType()),
        StructField("viajes", ArrayType(
            StructType(
                [
                    StructField("odigo_postal_origen", IntegerType(), True), 
                    StructField("codigo_postal_destino", IntegerType(), True), 
                    StructField("kilometros", DoubleType(), True), 
                    StructField("precio_kilometro", DoubleType(), True)
                ])
        ))    
    ])