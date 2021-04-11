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

SRC_FOLDER_PATH = 'resources/*.json'

# Read all json files from default directory.
riders_df = readJsonFilesFromPath(path=SRC_FOLDER_PATH,showdf=True, showSchema=True)

# Extract json properties into columns in main dataframe.
formatted_df = flatten_jsonColumn(riders_df, showdf=True, showSchema=True)

# Test command: spark-submit __main__.py