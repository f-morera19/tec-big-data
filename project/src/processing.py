"""
NAME
    processing.py

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
from pyspark.sql.window import Window
from pyspark.ml.feature import Imputer

# Format the sales dataframe accordingly.
def format_sales_dataframe(sales_source_df, showdf=False):
    """
    Params:
        sales_source_df (DataFrame): Sales dataframe to format.
    Returns: 
        Formatted sales Dataframe. 
    """

    # Imputer to substitute null scores by mean.
    imputer = Imputer(
        inputCols = ['user_score'], 
        outputCols=["user_score"]
    )

    sales_source_df = imputer.fit(sales_source_df).transform(sales_source_df)

    # Remove the unnecessary qualifying columns.
    sales_source_df = sales_source_df \
        .na.drop(subset=["name"]) \
        .na.drop(subset=["critic_score"]) \
        .drop("rank") \
        .drop("esrb_rating") \
        .drop("platform") \
        .drop("publisher") \
        .drop("developer") \
        .na.fill(value=0, subset=["total_shipped"]) \
        .na.fill(value=0, subset=["global_sales"]) \
        .na.fill(value=0, subset=["na_sales"]) \
        .na.fill(value=0, subset=["pal_sales"]) \
        .na.fill(value=0, subset=["jp_sales"]) \
        .na.fill(value=0, subset=["other_sales"])

    # Give proper format to the wanted columns.
    formatted_df = sales_source_df \
        .withColumn("name", F.trim(F.col("name"))) \
        .withColumn("formatted_name", F.regexp_replace(F.col("name"), "[^a-zA-Z0-9]", "")) \
        .withColumn('formatted_name', F.upper(F.col('formatted_name'))) \
        .withColumn(
            "total_shipped",
            F.when(F.col("total_shipped") == 0, F.col("global_sales")) \
            .otherwise(F.col("total_shipped"))) \
        .filter(F.col("total_shipped") != 0) \
        .withColumn(
            "year",
            F.col("year").cast(IntegerType())) \
        .withColumn("user_score", F.round(F.col("user_score"), 2)) \
        .drop("na_sales") \
        .drop("pal_sales") \
        .drop("jp_sales") \
        .drop("other_sales") \
        .drop("global_sales") \
        .drop("genre")

    if showdf: 
        formatted_df.show()

    return formatted_df

# Get the union of both postal codes types.
def format_critics_dataframe(critics_source_df, showdf=False):
    """
    Params:
        critics_source_df (DataFrame): Critics dataframe to format.
    Returns: 
        Formatted critics Dataframe. 
    """

    # Remove the unnecessary qualifying columns.
    critics_source_df = critics_source_df \
        .na.drop(subset=["name"]) \
        .na.drop(subset=["metascore"]) \
        .drop("attribute") \
        .drop("release_date") \
        .drop("link") \
        .drop("publisher") \
        .drop("developer") \
        .na.fill(value=0, subset=["critic_positive"]) \
        .na.fill(value=0, subset=["critic_neutral"]) \
        .na.fill(value=0, subset=["critic_negative"]) \
        .na.fill(value=0, subset=["user_positive"]) \
        .na.fill(value=0, subset=["user_neutral"]) \
        .na.fill(value=0, subset=["user_negative"]) \
        .na.fill(value="T", subset=["esrb_rating"]) \
        .withColumn("metacritic_user_score", F.trim(F.col("metacritic_user_score"))) \
        .withColumn('metacritic_user_score', F.upper(F.col('metacritic_user_score'))) \
        .filter(F.col("metacritic_user_score") != 'TBD')

    formatted_df = critics_source_df \
        .withColumn("name", F.trim(F.col("name"))) \
        .withColumn("metacritic_formatted_name", F.regexp_replace(F.col("name"), "[^a-zA-Z0-9]", "")) \
        .withColumn('metacritic_formatted_name', F.upper(F.col('metacritic_formatted_name'))) \
        .withColumn('genre', F.upper(F.col('genre'))) \
        .withColumn('genre', F.trim(F.col('genre'))) \
        .withColumn('players', F.upper(F.col('players'))) \
        .withColumn('players', F.trim(F.col('players'))) \
        .withColumn('esrb_rating', F.upper(F.col('esrb_rating'))) \
        .withColumn('esrb_rating', F.trim(F.col('esrb_rating'))) \
        .withColumn('platform', F.upper(F.col('platform'))) \
        .withColumn('platform', F.trim(F.col('platform'))) \
        .withColumn('isPCPlatform',
            F.when(F.col("platform") == "PC", 1)
            .otherwise(0)) \
        .withColumn('isX360Platform',
            F.when(F.col("platform") == "x360", 1)
            .otherwise(0)) \
        .withColumn('isOtherPlatform',
            F.when((F.col("platform") != "PC") & (F.col("platform") != "x360"), 1)
            .otherwise(0)) \
        .withColumn('isERating',
            F.when(F.col("esrb_rating") == "E", 1)
            .otherwise(0)) \
        .withColumn('isTRating',
            F.when(F.col("esrb_rating") == "T", 1)
            .otherwise(0)) \
        .withColumn('isMRating',
            F.when(F.col("esrb_rating") == "M", 1)
            .otherwise(0)) \
        .withColumn('isOtherRating',
            F.when((F.col("esrb_rating") != "E") & (F.col("esrb_rating") != "T") & (F.col("esrb_rating") != "M"), 1)
            .otherwise(0)) \
        .withColumn('isActionGenre',
            F.when(F.col("genre") == "ACTION", 1)
            .otherwise(0)) \
        .withColumn('isActionAdventureGenre',
            F.when(F.col("genre") == "ACTION ADVENTURE", 1)
            .otherwise(0)) \
        .withColumn('isOtherGenre',
            F.when((F.col("genre") != "ACTION") & (F.col("genre") != "ACTION ADVENTURE"), 1)
            .otherwise(0)) \
        .withColumn('isOnePlayerOnly',
            F.when(F.col("players") == "1 PLAYER", 1)
            .otherwise(0)) \
        .withColumn('hasOnlineMultiplayer',
            F.when(F.col("players") != 'NO ONLINE MULTIPLAYER', 1)
            .otherwise(0)) \
        .withColumn(
            "metacritic_user_score",
            F.col("metacritic_user_score").cast(IntegerType())) \
        .drop("genre") \
        .drop("players") \
        .drop("platform") \
        .drop("esrb_rating") 

    if showdf: 
        formatted_df.show()

    return formatted_df