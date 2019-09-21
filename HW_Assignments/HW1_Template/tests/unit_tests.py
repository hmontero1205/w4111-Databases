# I write and test methods one at a time.
# This file contains unit tests of individual methods.

from src.CSVDataTable import CSVDataTable
from src.RDBDataTable import RDBDataTable
import tests.csv_table_tests
import tests.rdb_table_tests
import os

def run_all_tests():
    tests.csv_table_tests.run_tests()
    tests.rdb_table_tests.run_tests()

if __name__ == "__main__":
    run_all_tests()
