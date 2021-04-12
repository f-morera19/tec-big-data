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
from src.data_processing import *

SRC_FOLDER_PATH = 'resources/*.json'

# Read all json files from default directory.
riders_df = readJsonFilesFromPath(path=SRC_FOLDER_PATH)

# Extract json properties into columns in main dataframe.
formatted_df = flatten_jsonColumn(riders_df, showdf=True)

# Postal Codes Metadata.
postal_codes_df = get_union_source_data(formatted_df)