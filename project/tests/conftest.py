"""
NAME
    conftest.py

DESCRIPTION   
    Unit test configuration for Spark.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021
"""

import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="module")
def spark_session():
    """A fixture to create a Spark Context to reuse across tests."""
    s = SparkSession.builder.appName('pytest-local-spark').master('local') \
        .getOrCreate()

    yield s
    s.stop()