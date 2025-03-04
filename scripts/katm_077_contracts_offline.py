import json
import pandas as pd
from time import time
from katm_connections import get_mongo_client
from katm_utils import insert_into_mssql, load_my_columns, \
                        get_numbers,map_dff_to_my_columns, max_number_find, \
                        select

start = time()
# get_nums_table='bronze.katm_077_contracts_offline'
# foreign_key = 'contracts_qty'
write_to_table = 'bronze.katm_077_contracts_offline'
columns_file = 'katm_077_contracts_fields.txt'
columns = load_my_columns(columns_file)

# numbers = [int(i) for i in get_numbers(get_nums_table, foreign_key, 10000)]
# max_num = max_number_find(get_nums_table)
# max_date = select('select max(cast(contract_date as date)) from bronze.katm_077_contracts_offline')[0].strftime('%Y-%m-%d')

client = get_mongo_client()
db = client['katm']
task_collection = db['katm_cache']

query = {
    'reports.077.contracts.contract': {'$exists': True}
    # ,'number': {'$eq': 8141290}
    # ,'number': {'$gt': max_num}
    # ,'number': {'$gt': 1243100, '$lt': 1243200}
    # ,'number': {'$in': numbers}
    # , 'reports.077.contracts.contract.contract_date':{'$gte':max_date}
}
projection = {
    '_id': 1,
    'reports.077.contracts.contract': 1
}
docs = task_collection.find(query, projection)
i = 0
rows=[]
for doc in docs:
    if i% 100_000 == 0:
        df = pd.DataFrame(rows)
        final_df = map_dff_to_my_columns(df, columns)
        insert_into_mssql(final_df, write_to_table)
        rows = []    
    
    i+=1
    _id = str(doc.get('_id')) 
    number = doc.get('number')
    fields = doc.get('reports', {}).get('077', {}).get('contracts', {}).get('contract', [])
    if isinstance(fields, dict):
        fields = [fields]
    elif fields is None:
        fields = []
    for field in fields:
        # filtered_field = {k: v for k, v in field.items() if not isinstance(v, (dict, list))}
        row = {'_id': _id, **field}
        rows.append(row)
        
df = pd.DataFrame(rows)
final_df = map_dff_to_my_columns(df, columns)
insert_into_mssql(final_df, write_to_table)
end = time()
print(f"script took {end-start} seconds")