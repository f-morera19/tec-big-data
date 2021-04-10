"""
NAME
    __main__.py

DESCRIPTION   
    Program entry point for homework 2.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021
"""
import argparse
from pyspark.sql import SparkSession
from src.read_input import *

# Read all json files from default directory.
riders_df = readJsonFilesFromPath(show=True)

# Extract json properties into columns in main dataframe.


# Test command: spark-submit __main__.py