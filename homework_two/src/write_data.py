"""
NAME
    write_data.py

DESCRIPTION   
    Program entry point for homework 2.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021
"""

from pyspark.sql import SparkSession

# Write a CSV to putput path from dataframe.
def write_csv_to_output(
    src_dataframe, 
    output_path):
    src_dataframe.write.csv(output_path)
    print(f"{output_path} succesfully written in memory.")