
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
        (508411730, 620,24),
        (670106829, 281.5)
        ],
        ['id', 'sum(distance)'])

    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()