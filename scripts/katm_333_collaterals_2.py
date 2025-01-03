import json
import pandas as pd
from time import time
from connections import get_mongo_client
from my_utils import insert_into_mssql, load_my_columns, \
                                        get_numbers_2, map_dff_to_my_columns

start = time()

write_to_table = 'bronze.katm_077_scoring'
columns_file = 'katm_077_scoring_fields.txt'
columns = load_my_columns(columns_file)