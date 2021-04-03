"""
NAME
    validator.py

DESCRIPTION   
    Validation helper methods.

Student: Fabian Morera Gutierrez.
Course: Big Data.
Instituto Tecnologico de Costa Rica.
2021
"""

#   Validate the required source data paths.
def validatePaths(c_path, a_path, r_path):
    if c_path is None or not c_path:
        raise TypeError(f'Variable {c_path} can not be null.')

    if a_path is None or not a_path:
        raise TypeError(f'Variable {a_path} can not be null.')
    
    if r_path is None or not r_path:
        raise TypeError(f'Variable {r_path} can not be null.')