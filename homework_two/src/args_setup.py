"""
NAME
    args_setup.py

DESCRIPTION   
    Program initial setup helper methods.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021
"""

import argparse

RESOURCES_FOLDER_PATH = 'resources/'

# Get program entry arguments from console.
def get_args():

    parser = argparse.ArgumentParser(
        description='Homework_two: Process Diber DataFrames.',
        epilog="Note: All paths must be relative to the resources folder (i.e. files must \
            be located in that specific folder only to work.) By default, if no argument is \
            provided, all files from the resources folder will be taken in consideration.")

    parser.add_argument('files', metavar='source_path', type=str, nargs='*',
                        help='one or more strings as source input path for the program. ')

    args = parser.parse_args()
    
    formatted_inputs = map(lambda x: RESOURCES_FOLDER_PATH + x, args.files)
    
    return list(formatted_inputs)