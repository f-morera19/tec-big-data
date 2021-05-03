"""
NAME
    test_vgcritics_processing.py

DESCRIPTION   
    Unit tests for vg critics processing functionality.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021

"""

# Imports.
from src.processing import *
from schemas.schemas import *

# Format Critics Dataframe with no null values, Pc, T Rating and online multiplayer.
def test_format_critics_dataframe_whenNoNullValuesPcTRatingOnlineAvailable(spark_session):

    critics_data = [
        ('Quake','PC','id Software','id Software','Action','1 Player','T', '','Jun 22, 1996','/game/pc/quake',9,0,0,94,84,4,1,8.8)
        ]

    critics_df = spark_session.createDataFrame(
        critics_data,
        critics_schema)

    # Generated DataFrame by system's logic.
    actual_df = format_critics_dataframe(critics_df)

    # Expected result.
    expected_df = spark_session.createDataFrame(
        [
            ('Quake',9,0,0,94,84,4,1,8.8,'QUAKE',1,0,0,0,1,0,0,1,0,0,1,1)
        ],
        formatted_critics_schema)
    
    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()

# Format Critics Dataframe with no Other Platform, M Rating and No online multiplayer.
def test_format_critics_dataframe_whenNoNullValuesOtherPlatformMRatingNoMultiplayer(spark_session):

    critics_data = [
        ('Quake','Wii','id Software','id Software','Action','No online multiplayer','M', '','Jun 22, 1996','/game/pc/quake',9,0,0,94,84,4,1,8.8)
        ]

    critics_df = spark_session.createDataFrame(
        critics_data,
        critics_schema)

    # Generated DataFrame by system's logic.
    actual_df = format_critics_dataframe(critics_df)

    # Expected result.
    expected_df = spark_session.createDataFrame(
        [
            ('Quake',9,0,0,94,84,4,1,8.8,'QUAKE',0,0,1,0,0,1,0,1,0,0,0,0)
        ],
        formatted_critics_schema)
    
    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()

# Format Critics Dataframe with null name.
def test_format_critics_dataframe_whenNullNameValue(spark_session):

    critics_data = [
        (None,'PC','id Software','id Software','Action','1 Player','T', '','Jun 22, 1996','/game/pc/quake',9,0,0,94,84,4,1,8.8),
        ('Quake','PC','id Software','id Software','Action','1 Player','T', '','Jun 22, 1996','/game/pc/quake',9,0,0,94,84,4,1,8.8)
        ]

    critics_df = spark_session.createDataFrame(
        critics_data,
        critics_schema)

    # Generated DataFrame by system's logic.
    actual_df = format_critics_dataframe(critics_df)

    # Expected result.
    expected_df = spark_session.createDataFrame(
        [
            ('Quake',9,0,0,94,84,4,1,8.8,'QUAKE',1,0,0,0,1,0,0,1,0,0,1,1)
        ],
        formatted_critics_schema)
    
    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()

# Format Critics Dataframe with null metascore.
def test_format_critics_dataframe_whenNullMetascoreValue(spark_session):

    critics_data = [
        ('Quake','PC','id Software','id Software','Action','1 Player','T', '','Jun 22, 1996','/game/pc/quake',9,0,0,None,84,4,1,8.8)
        ]

    critics_df = spark_session.createDataFrame(
        critics_data,
        critics_schema)

    # Generated DataFrame by system's logic.
    actual_df = format_critics_dataframe(critics_df)

    # Expected result.
    expected_df = spark_session.createDataFrame(
        [],
        formatted_critics_schema)
    
    actual_df.show()
    expected_df.show()

    assert actual_df.collect() == expected_df.collect()