"""
NAME
    test_final.py

DESCRIPTION   
    Unit tests to fetch top N cyclists by
    province. Both by total distance and the
    mean of distance by day.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021

"""

from transactions.data_aggregation import *
from schemas.main_schemas import *
import datetime

# Get the top N = 3 of the cyclists by their total distance.
def test_getNTopCyclists_ByTotalDistance_AggregateOfPatients(spark_session):
    
    N = 3

    aggr_patients_data = [
        (508411730, 620.24),
        (670106829, 123.5),
        (451473854, 471.19),
        (810726820, 512.78),
        (349549806, 390.8),
        (799256246, 321.98)]

    aggr_patients_df = spark_session.createDataFrame(
        aggr_patients_data,
        sum_schema_cyclist)

    # Generated DataFrame by system's logic.
    actual_df = aggr_patients_df.orderBy('sum(distance)',ascending=False).limit(N)

    # Expected result.
    expected_df = spark_session.createDataFrame([
        (508411730, 620.24),
        (810726820, 512.78),
        (451473854, 471.19)],
        sum_schema_cyclist)
    
    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()