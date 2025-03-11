import json
import pandas as pd
from time import time
from katm_connections import get_mongo_client
from katm_utils import insert_into_mssql_tqdm, load_my_columns, \
                        map_dff_to_my_columns, max_number_find_with_cast


start = time()
write_to_table = 'bronze.asbt_credit_list_online_microloan'
columns_file = 'asbt_credit_list_online_microloan_fields.txt'
columns = load_my_columns(columns_file)
max_num = max_number_find_with_cast(write_to_table)

client = get_mongo_client()
db = client['task']
task_collection = db['task']

query = {
    'data.asbt_credit_list.return.object': {'$exists': True}
    ,'number': {'$gt': max_num}
}
projection = {
    'number': 1,
    'data.asbt_credit_list.return.object': 1
}
docs = task_collection.find(query, projection)
i = 0
rows=[]
for doc in docs:
    if i% 10_000 == 0:
        df = pd.DataFrame(rows)
        final_df = map_dff_to_my_columns(df, columns)
        insert_into_mssql_tqdm(final_df, write_to_table)
        rows = []    
    
    i+=1
    number = doc.get('number')
    fields = doc.get('data', {}).get('asbt_credit_list', {}).get('return', {}).get('object', [])
    if isinstance(fields, dict):
        fields = [fields]
    elif fields is None:
        fields = []
    for field in fields:
        # filtered_field = {k: v for k, v in field.items() if not isinstance(v, (dict, list))}
        row = {'number': number, **field}
        rows.append(row)
        
df = pd.DataFrame(rows)
final_df = map_dff_to_my_columns(df, columns)
insert_into_mssql_tqdm(final_df, write_to_table)
end = time()
print(f"script took {end-start} seconds")