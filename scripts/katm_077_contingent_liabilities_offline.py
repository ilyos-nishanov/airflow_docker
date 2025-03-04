import json
import pandas as pd
from time import time
from katm_connections import get_mongo_client
from katm_utils import insert_into_mssql, load_my_columns, \
                        get_numbers,map_dff_to_my_columns, max_number_find

start = time()
# get_nums_table='bronze.katm_077_contingent_liabilities'
# foreign_key = 'contingent_liabilities_qty'
write_to_table = 'bronze.katm_077_contingent_liabilities_offline'
columns_file = 'katm_077_contingent_liabilities_columns.txt'
columns = load_my_columns(columns_file)

# numbers = [int(i) for i in get_numbers(get_nums_table, foreign_key, 10000)]
# max_num = max_number_find(write_to_table)

client = get_mongo_client()
db = client['katm']
task_collection = db['katm_cache']

query = {
    'reports.077.contingent_liabilities.contingent_liability': {'$exists': True}
    # ,'number': {'$eq': 5651095}
    # ,'number': {'$gt': max_num}
    # ,'number': {'$gt': 1243100, '$lt': 1243200}
    # ,'number': {'$in': numbers}   
}
projection = {
    '_id': 1,
    'reports.077.contingent_liabilities.contingent_liability': 1
}
docs = task_collection.find(query, projection)#.sort('number', -1)
for doc in docs:
    rows = []
    _id = str(doc.get('_id'))
    fields = doc.get('reports', {}).get('077', {}).get('contingent_liabilities', {}).get('contingent_liability', [])
    if isinstance(fields, dict):
        fields = [fields]
    elif fields is None:
        fields = []
    for field in fields:
        row = {'_id': _id, **field}
        rows.append(row)

    df = pd.DataFrame(rows)
    final_df = map_dff_to_my_columns(df, columns)
    insert_into_mssql(final_df, write_to_table)
end = time()
print(f"script took {end-start} seconds")