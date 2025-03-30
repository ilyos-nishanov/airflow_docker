import json
import pandas as pd
from time import time
from katm_connections import get_mongo_client
from katm_utils import insert_into_mssql, load_my_columns, \
                        get_numbers,map_dff_to_my_columns, max_number_find
write_to_table = '_id_for_onl_app'
client = get_mongo_client()
db = client['task']
task_collection = db['task']

query = {
    'number': {'$gt': 9725580}
}
projection = {
    'number': 1,
    '_id': 1
}
docs = task_collection.find(query, projection)
rows=[]
for doc in docs:
    _id = str(doc.get('_id')) 
    number = doc.get('number')
    row = {'_id': _id, 'number': number}
    rows.append(row)
df = pd.DataFrame(rows)
insert_into_mssql(df, write_to_table)