"""
NAME
    data_aggregation.py

DESCRIPTION   
    Logic for data aggregation.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021

"""

from pyspark.sql import SparkSession

def aggregateByCyclist(src_df):
    # Create new dataframe by grouping the data by cyclist 
    # using the sum function.
    agg_total_dist = src_df.na.drop().groupBy("id").sum("distance").orderBy("id")
    return agg_total_dist

def aggregateByRoute(src_df):
    # Create new dataframe by grouping the data by route name 
    # using the sum function.
    agg_total_dist = src_df.na.drop().groupBy("route_name").sum("distance").orderBy("route_name")
    return agg_total_dist

def aggregateByProvince(src_df):
    # Create new dataframe by grouping the data by province 
    # using the sum function.
    agg_total_dist = src_df.na.drop().groupBy("province").sum("distance").orderBy("province")
    return agg_total_dist

def aggregateByDate(src_df):
    # Create new dataframe by grouping the data by date 
    # using the sum function.
    agg_total_dist = src_df.na.drop().groupBy("date").sum("distance").orderBy("date")
    return agg_total_dist