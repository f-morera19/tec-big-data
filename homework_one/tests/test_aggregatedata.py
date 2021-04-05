
"""
NAME
    test_aggregatedata.py

DESCRIPTION   
    Unit tests for data union functionality.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021

"""

from transactions.data_aggregation import *
from schemas.main_schemas import *
import datetime

def test_aggregateByCyclist_OneOrMoreValuesForEachCyclist(spark_session):

    union_data = [
        (508411730, 'Chrystel Horribine', 'San Jose', 2, 'Bike Adventure Park', 338.74, datetime.date(2020,5,8)),
        (508411730, 'Chrystel Horribine', 'San Jose', 1, 'Bike Park RIO PERDIDO', 281.5, datetime.date(2020,5,9)),
        (670106829, 'Jeddy Tardiff', 'Heredia', 1, 'Bike Park RIO PERDIDO', 281.5, datetime.date(2021,1,10))]
    
    union_data_df = spark_session.createDataFrame(
        union_data,
        cyclists_activities_routes_schema)

    # Generated DataFrame by system's logic.
    actual_df = aggregateByCyclist(union_data_df)

    # Expected result.
    expected_df = spark_session.createDataFrame([
        (508411730, 620.24),
        (670106829, 281.5)],
        sum_schema_cyclist)

    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()

def test_aggregateByRoute_OneOrMoreValuesForEachRoute(spark_session):

    union_data = [
        (508411730, 'Chrystel Horribine', 'San Jose', 2, 'Bike Adventure Park', 222.74, datetime.date(2020,5,8)),
        (508411730, 'Chrystel Horribine', 'San Jose', 1, 'Bike Park RIO PERDIDO', 309.5, datetime.date(2020,5,9)),
        (670106829, 'Jeddy Tardiff', 'Heredia', 1, 'Bike Park RIO PERDIDO', 213.17, datetime.date(2021,1,10))]
    
    union_data_df = spark_session.createDataFrame(
        union_data,
        cyclists_activities_routes_schema)

    # Generated DataFrame by system's logic.
    actual_df = aggregateByRoute(union_data_df)

    # Expected result.
    expected_df = spark_session.createDataFrame([
        ('Bike Adventure Park', 222.74),
        ('Bike Park RIO PERDIDO', 522.67)],
        sum_schema_route)

    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()

def test_aggregateByProvince_OneOrMoreValuesForEachProvince(spark_session):

    union_data = [
        (508411730, 'Chrystel Horribine', 'San Jose', 2, 'Bike Adventure Park', 222.74, datetime.date(2020,5,8)),
        (508411730, 'Chrystel Horribine', 'San Jose', 1, 'Bike Park RIO PERDIDO', 309.5, datetime.date(2020,5,9)),
        (670106829, 'Jeddy Tardiff', 'Heredia', 1, 'Bike Park RIO PERDIDO', 213.17, datetime.date(2021,1,10))]
    
    union_data_df = spark_session.createDataFrame(
        union_data,
        cyclists_activities_routes_schema)

    # Generated DataFrame by system's logic.
    actual_df = aggregateByProvince(union_data_df)

    # Expected result.
    expected_df = spark_session.createDataFrame([
        ('Heredia', 213.17),
        ('San Jose', 532.24)],
        sum_schema_province)

    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()

def test_aggregateByDate_OneOrMoreValuesForEachDate(spark_session):

    union_data = [
        (508411730, 'Chrystel Horribine', 'San Jose', 2, 'Bike Adventure Park', 100.74, datetime.date(2020,5,9)),
        (508411730, 'Chrystel Horribine', 'San Jose', 1, 'Bike Park RIO PERDIDO', 123.5, datetime.date(2020,5,9)),
        (670106829, 'Jeddy Tardiff', 'Heredia', 1, 'Bike Park RIO PERDIDO', 258.17, datetime.date(2021,1,10))]
    
    union_data_df = spark_session.createDataFrame(
        union_data,
        cyclists_activities_routes_schema)

    # Generated DataFrame by system's logic.
    actual_df = aggregateByDate(union_data_df)

    # Expected result.
    expected_df = spark_session.createDataFrame([
        (datetime.date(2020,5,9), 224.24),
        (datetime.date(2021,1,10), 258.17)],
        sum_schema_date)

    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()