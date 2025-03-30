import json
import pandas as pd
from time import time
from datetime import datetime
from katm_connections import get_mongo_client
from katm_utils import  insert_into_mssql_tqdm,\
                        get_numbers, map_dff_to_my_columns, \
                        max_number_find, load_my_columns
start = time()
write_to_table = 'bronze.employer'
get_nums_table = 'bronze.employer'
columns_file = 'employer_fields.txt'
columns = load_my_columns(columns_file)
# max_num = max_number_find(get_nums_table)
client = get_mongo_client()
db = client['task']
task_collection = db['task']

query = {
    'data.uzasbo_salary.result.data.lastEmployer': {'$exists': True}
    # ,'number':{"$gt": max_num}
    ,'created': {'$gt': datetime(2025, 1, 1)}
}
projection = {
    '_id':1,
    'number': 1,
    'data.uzasbo_salary.result.data.lastEmployer': 1
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
    _id = str(doc.get('_id')) 
    number = doc.get('number')
    fields = doc.get('data', {}).get('uzasbo_salary', {}).get('result',{}).get('data', {}).get('lastEmployer', {})
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
insert_into_mssql_tqdm(df, write_to_table)
end = time()
print(f"script took {end-start} seconds")