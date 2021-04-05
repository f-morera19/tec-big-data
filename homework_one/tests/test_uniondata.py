"""
NAME
    test_uniondata.py

DESCRIPTION   
    Unit tests for data union functionality.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021

"""

from transactions.data_union import *
from schemas.main_schemas import *
import datetime

def test_union_cyclist_activity_noDuplicates_AllHaveMatchingValues(spark_session):

    # Mock-data for cyclists and activities
    cyclists_data = [
        (670106829, 'Jeddy Tardiff', 'Heredia'),
        (508411730, 'Chrystel Horribine', 'San Jose')]

    activities_data = [
        (1, 670106829, datetime.date(2021,1,10)),
        (2, 508411730, datetime.date(2020,5,8))]

    cyclists_df = spark_session.createDataFrame(
        cyclists_data,
        cyclists_schema)

    activities_df = spark_session.createDataFrame(
        activities_data,
        activities_schema)

    # Generated DataFrame by system's logic.
    actual_df = cyclists_activities_Union(
        cyclists_df, 
        activities_df)

    # Expected result.
    expected_df = spark_session.createDataFrame([
        (508411730, 'Chrystel Horribine', 'San Jose', 2, datetime.date(2020,5,8)),
        (670106829, 'Jeddy Tardiff', 'Heredia', 1, datetime.date(2021,1,10))
        ],
        ['id', 'full_name', 'province', 'route_code','date'])

    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()

def test_union_cyclist_activity_noDuplicates_NotAllHaveMatchingValues(spark_session):

    # Mock-data for cyclists and activities
    cyclists_data = [
        (670106829, 'Jeddy Tardiff', 'Heredia'),
        (508411730, 'Chrystel Horribine', 'San Jose')]

    activities_data = [
        (1, 670106829, datetime.date(2021,1,10)),
        (2, 908411730, datetime.date(2020,5,8))]

    cyclists_df = spark_session.createDataFrame(
        cyclists_data,
        cyclists_schema)

    activities_df = spark_session.createDataFrame(
        activities_data,
        activities_schema)

    # Generated DataFrame by system's logic.
    actual_df = cyclists_activities_Union(
        cyclists_df, 
        activities_df)

    # Expected result.
    expected_df = spark_session.createDataFrame([
        (508411730, 'Chrystel Horribine', 'San Jose', None, None),
        (670106829, 'Jeddy Tardiff', 'Heredia', 1, datetime.date(2021,1,10))
        ],
        ['id', 'full_name', 'province', 'route_code','date'])

    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()

def test_union_cyclist_activity_Duplicates_AllHaveMatchingValues(spark_session):

    # Mock-data for cyclists and activities
    cyclists_data = [
        (670106829, 'Jeddy Tardiff', 'Heredia'),
        (508411730, 'Chrystel Horribine', 'San Jose')]

    activities_data = [
        (1, 670106829, datetime.date(2021,1,10)),
        (2, 670106829, datetime.date(2020,5,8))]

    cyclists_df = spark_session.createDataFrame(
        cyclists_data,
        cyclists_schema)

    activities_df = spark_session.createDataFrame(
        activities_data,
        activities_schema)

    # Generated DataFrame by system's logic.
    actual_df = cyclists_activities_Union(
        cyclists_df, 
        activities_df)

    # Expected result.
    expected_df = spark_session.createDataFrame([
        (508411730, 'Chrystel Horribine', 'San Jose', None, None),
        (670106829, 'Jeddy Tardiff', 'Heredia', 1, datetime.date(2021,1,10)),
        (670106829, 'Jeddy Tardiff', 'Heredia', 2, datetime.date(2020,5,8))
        ],
        ['id', 'full_name', 'province', 'route_code','date'])

    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()

def test_union_cyclists_activities_routes_noDuplicates_AllHaveMatchingValues(spark_session):

    cyclists_activities_data = [
        (670106829, 'Jeddy Tardiff', 'Heredia', 1, datetime.date(2021,1,10)),
        (508411730, 'Chrystel Horribine', 'San Jose', 2, datetime.date(2020,5,8))]

    routes_data = [
        (1, 'Bike Park RIO PERDIDO', 281.5),
        (2, 'Bike Adventure Park', 338.74)]

    cyclists_activities_df = spark_session.createDataFrame(
        cyclists_activities_data,
        cyclists_activities_schema)

    routes_df = spark_session.createDataFrame(
        routes_data,
        routes_schema)

    # Generated DataFrame by system's logic.
    actual_df = cyclists_activities_routes_Union(
        cyclists_activities_df, 
        routes_df)

    # Expected result.
    expected_df = spark_session.createDataFrame([
        (508411730, 'Chrystel Horribine', 'San Jose', 2, datetime.date(2020,5,8), 'Bike Adventure Park', 338.74),
        (670106829, 'Jeddy Tardiff', 'Heredia', 1, datetime.date(2021,1,10), 'Bike Park RIO PERDIDO', 281.5)
        ],
        ['id', 'full_name', 'province', 'route_code','date','name', 'distance'])

    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()

def test_union_cyclists_activities_routes_noDuplicates_NotAllHaveMatchingValues(spark_session):

    cyclists_activities_data = [
        (670106829, 'Jeddy Tardiff', 'Heredia', 2, datetime.date(2021,1,10)),
        (508411730, 'Chrystel Horribine', 'San Jose', 1, datetime.date(2020,5,8))]

    routes_data = [
        (1, 'Bike Park RIO PERDIDO', 281.5),
        (3, 'Bike Adventure Park', 338.74)]

    cyclists_activities_df = spark_session.createDataFrame(
        cyclists_activities_data,
        cyclists_activities_schema)

    routes_df = spark_session.createDataFrame(
        routes_data,
        routes_schema)

    # Generated DataFrame by system's logic.
    actual_df = cyclists_activities_routes_Union(
        cyclists_activities_df, 
        routes_df)

    # Expected result.
    expected_df = spark_session.createDataFrame([
        (508411730, 'Chrystel Horribine', 'San Jose', 1, datetime.date(2020,5,8), 'Bike Park RIO PERDIDO', 281.5),
        (670106829, 'Jeddy Tardiff', 'Heredia', 2, datetime.date(2021,1,10), None, None)
        ],
        ['id', 'full_name', 'province', 'route_code','date','name', 'distance'])

    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()

def test_union_cyclist_activities_routes_noDuplicates_AllHaveMatchingValues(spark_session):

    # Mock-data for cyclists, activities and routes.
    cyclists_data = [
        (670106829, 'Jeddy Tardiff', 'Heredia'),
        (508411730, 'Chrystel Horribine', 'San Jose')]

    activities_data = [
        (1, 670106829, datetime.date(2021,1,10)),
        (2, 508411730, datetime.date(2020,5,8))]

    routes_data = [
        (1, 'Bike Park RIO PERDIDO', 281.5),
        (2, 'Bike Adventure Park', 338.74)]

    # Create dataframes.
    routes_df = spark_session.createDataFrame(
        routes_data,
        routes_schema)

    cyclists_df = spark_session.createDataFrame(
        cyclists_data,
        cyclists_schema)

    activities_df = spark_session.createDataFrame(
        activities_data,
        activities_schema)
        
    # Generated DataFrame by system's logic.
    actual_df = union(
        cyclists_df, 
        routes_df,
        activities_df)

    # Expected result.
    expected_df = spark_session.createDataFrame([
        (508411730, 'Chrystel Horribine', 'San Jose', 2, 'Bike Adventure Park', 338.74, datetime.date(2020,5,8), ),
        (670106829, 'Jeddy Tardiff', 'Heredia', 1, 'Bike Park RIO PERDIDO', 281.5, datetime.date(2021,1,10))
        ],
        cyclists_activities_routes_schema)

    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()