"""
NAME
    write_data.py

DESCRIPTION   
    Write dataframe into database instance.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021
"""
from pyspark.sql import SparkSession

# Default parameters for DB access.
OVERWRITE_MODE = 'overwrite'
DEF_USER_NAME = 'postgres'
DEF_USER_PASSWORD = 'testPassword'

# Save dataframe to DB on specific table.
def save_data_DB(source_df, DB_name):
    """
    Params:
        source_df (DataFrame): Dataframe to be stored.
        DB_name (String): Name of table to store data at.
    Returns: 
        void.
    """

    source_df \
        .write \
        .format("jdbc") \
        .mode(OVERWRITE_MODE) \
        .option("url", "jdbc:postgresql://host.docker.internal:5433/postgres") \
        .option("user", DEF_USER_NAME) \
        .option("password", DEF_USER_PASSWORD) \
        .option("dbtable", DB_name) \
        .save()

    print("Clean flights data saved!\n")