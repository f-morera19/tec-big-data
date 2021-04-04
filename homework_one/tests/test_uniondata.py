from transactions.data_union import *

def test_union_cyclist_activity_noDuplicates_AllHaveMatchingValues(spark_session):

    # Mock-data for cyclists and activities
    cyclists_data = [
        (670106829, 'Jeddy Tardiff', 'Heredia'),
        (508411730, 'Chrystel Horribine', 'San Jose')]

    activities_data = [
        (1, 670106829, '2021-01-10'),
        (2, 508411730, '2020-05-08')]

    cyclists_df = spark_session.createDataFrame(
        cyclists_data,
        ['id', 'full_name', 'province'])

    activities_df = spark_session.createDataFrame(
        activities_data,
        ['route_code', 'cyclist_id', 'date'])

    # Generated DataFrame by system's logic.
    actual_df = cyclists_activities_Union(
        cyclists_df, 
        activities_df)

    # Expected result.
    expected_df = spark_session.createDataFrame([
        (670106829, 'Jeddy Tardiff', 'Heredia', 1, '2021-01-10'),
        (508411730, 'Chrystel Horribine', 'San Jose', 2, '2020-05-08')
        ],
        ['id', 'full_name', 'province', 'route_code','date'])

    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()
