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
    
    return args.c_path, args.a_path, args.r_path