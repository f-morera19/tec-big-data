"""
NAME
    setup.py

DESCRIPTION   
    Program initial setup helper methods.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021
"""

import argparse

MOCK_DATA_FOLDER_PATH = 'mock_data/'

def get_args():

    parser = argparse.ArgumentParser(
        description='Homework_one: Process cyclists, routes and activities DataFrames.',
        epilog="Note: All paths must be relative to the mock_data folder (i.e. files must \
            be located in that specific folder only.)")

    parser.add_argument('c_path', metavar='cyclists_path', type=str,
                        help='a string for the cyclists data path.')
    parser.add_argument('a_path', metavar='activities_path', type=str,
                        help='a string for the activities data path.')
    parser.add_argument('r_path', metavar='routes_path', type=str,
                        help='a string for the routes data path.')

    args = parser.parse_args()
    
    # add the mock_data folder to path.
    c_path = MOCK_DATA_FOLDER_PATH + args.c_path
    a_path = MOCK_DATA_FOLDER_PATH + args.a_path
    r_path = MOCK_DATA_FOLDER_PATH + args.r_path

    return c_path, a_path, r_path