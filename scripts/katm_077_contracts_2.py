import json
import pandas as pd
from time import time
from connections import get_mongo_client
from my_utils import insert_into_mssql, load_my_columns, \
                        get_numbers,map_dff_to_my_columns, max_number_find

start = time()
get_nums_table='bronze.katm_077_contracts'
foreign_key = 'contracts_qty'
write_to_table = 'bronze.katm_077_contracts'
columns_file = 'katm_077_contracts_fields.txt'
columns = load_my_columns(columns_file)

numbers = [int(i) for i in get_numbers(get_nums_table, foreign_key, 10000)]

client = get_mongo_client()
db = client['task']
task_collection = db['task']

query = {
    'data.katm_077.return.data.contracts.contract': {'$exists': True}
    ,'number': {'$eq': 8141290}
    # ,'number': {'$gt': max_num}
    # ,'number': {'$gt': 1243100, '$lt': 1243200}
    # ,'number': {'$in': numbers}
}
projection = {
    'number': 1,
    'data.katm_077.return.data.contracts.contract': 1
}
docs = task_collection.find(query, projection)
for doc in docs:
    rows=[]
    _id = str(doc.get('_id')) 
    number = doc.get('number')
    fields = doc.get('data', {}).get('katm_077', {}).get('return', {}).get('data', {}).get('contracts', {}).get('contract', [])
    if isinstance(fields, dict):
        fields = [fields]
    elif fields is None:
        fields = []
    for field in fields:
        # filtered_field = {k: v for k, v in field.items() if not isinstance(v, (dict, list))}
        row = {'_id': _id, 'number': number, **field}
        rows.append(row)
        
    df = pd.DataFrame(rows)
    final_df = map_dff_to_my_columns(df, columns)
    insert_into_mssql(final_df, write_to_table)
end = time()
print(f"script took {end-start} seconds")