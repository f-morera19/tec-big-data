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

# Format dataframe to only include fields
# related to the total income produce by
# each postal code.
def get_total_income_formated_df(source_df):
    """
    Params:
        source_df (DataFrame): dataframe to 
        format.
    Returns: 
        DataFrame: form codigo_postal, type, total_income.
    """

    result_df = source_df.select(
        F.col("codigo_postal"),
        F.col("type"),
        F.col("total_income")
    )
    return result_df

# Format dataframe to only include fields
# related to the total amount of 
# each postal code.
def get_total_amount_formated_df(source_df):
    """
    Params:
        source_df (DataFrame): dataframe to 
        format.
    Returns: 
        DataFrame: form codigo_postal, type, total_amount.
    """
    result_df = source_df.select(
        F.col("codigo_postal"),
        F.col("type"),
        F.col("total_amount")
    )
    return result_df

# Generate varios metrics for Diber data.
def get_metrics(source_df):
    """
    Params:
        source_df (DataFrame): dataframe with the form:
        user_id, codigo_postal_destino, odigo_postal_origen, 
        kilometros, precio_kilometro.
    Returns: 
        DataFrame: form metric_type, value.
    """

    spark = SparkSession.builder.appName("Read Transactions").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([ \
        StructField("metric_type", StringType(), False), \
        StructField("value", StringType(), False)
    ])

    most_distance_driver_id = get_most_distance_driver(source_df)
    print("Max driver distance: ", most_distance_driver_id)


    metrics_df = spark.createDataFrame(data=[],schema=schema)
    return metrics_df

# Get the driver with the most distance
def get_most_distance_driver(source_df):
    
    return source_df\
        .groupBy("user_id")\
        .sum("kilometros")\
        .sort(F.col("sum(kilometros)").desc())\
        .first()[0]