"""
NAME
    data_processing.py

DESCRIPTION   
    Process data into aggregates with solicited information.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import * 

# Get the union of both postal codes types.
def get_union_source_data(source_df, showdf=False):
    """
    Params:
        source_df (DataFrame): dataframe to 
        unite data from.
    Returns: 
        DataFrame: form codigo_postal, income, amount.
    """

    cpo_df = source_df\
        .groupBy("codigo_postal_origen")\
        .agg(
            F.sum(F.col("kilometros") * F.col("precio_kilometro")).alias("total_income"),
            F.count(F.col("codigo_postal_origen")).alias("total_amount"))\
        .withColumn("type", F.lit("ORIGEN"))\
        .withColumnRenamed("codigo_postal_origen", "codigo_postal")

    cpd_df = source_df\
        .groupBy("codigo_postal_destino")\
        .agg(
            F.sum(F.col("kilometros") * F.col("precio_kilometro")).alias("total_income"),
            F.count(F.col("codigo_postal_destino")).alias("total_amount"))\
        .withColumn("type", F.lit("DESTINO"))\
        .withColumnRenamed("codigo_postal_destino", "codigo_postal")

    result_df = cpo_df.union(cpd_df).orderBy("codigo_postal")

    if showdf:
        result_df.show()
    
    return result_df

# Test command: spark-submit __main__.py