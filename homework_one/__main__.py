"""
NAME
    __main__.py

DESCRIPTION   
    Program entry point.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021
"""

import argparse
from helpers.args_setup import *
from transactions.data_input import *

#   Set the input paths.
cyclists_path, activities_path, routes_path = get_args()

#   Read the source Cyclists, Activities and Routes csv files.
cyclists_df = readCsvIntoDataframe(
    source=cyclists_path,
    csv_schema)

#   Create union DF from source DTs.

#   Create the partial aggregation DTs.

#   Create functions that return the top N of cyclists. (low priority)