"""
NAME
    data_union.py

DESCRIPTION   
    All logic related to handling the union of DataFrames, specifically
    for cyclists, routes and activities data. 

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021
"""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, udf
from pyspark.sql.types import (DateType, IntegerType, FloatType, StructField,
                               StructType, TimestampType)

#   Create union dt from input ones, according to predefined logic.
def union(cyclists, routes, activities):
    """
    Params:
        cyclists (DataFrame): Cyclists' data.
        routes (DataFrame): Routes' data.
        activities (DataFrame): Activities' data.
    
    Returns:
        DataFrame: Union result.
    """

    validateInputDTs(cyclists,routes,activities)

    #   Create left join with activies DT.
    left_join_cyclists_activities_dt = cyclists.join(
        activities, 
        cyclists.id == activities.cyclist_id,
        how='left')

    #   Create left join with the previous custructed DT and Routes DT. 
    left_join_activities_routes_dt = \
        left_join_cyclists_activities_dt.join(
            routes,
            left_join_cyclists_activities_dt.route_code == routes.code,
            how='left')

    #   Build and return the resulting union dataframe.
    union_dt = \
        left_join_activities_routes_dt.select(
            col('id'),
            col('full_name'),
            col('province'),
            col('route_code'),
            col('name').alias('route_name'),
            col('distance'),
            col('date')
        )

    return union_dt

#   Validate the source dataframes.
def validateInputDTs(cyclists, routes, activities):

    if cyclists is None:
        raise TypeError(f'Variable {cyclists} can not be null.')

    if routes is None:
        raise TypeError(f'Variable {routes} can not be null.')
    
    if activities is None:
        raise TypeError(f'Variable {activities} can not be null.')
