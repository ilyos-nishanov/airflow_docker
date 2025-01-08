import json
import pandas as pd
from time import time
from connections import get_mongo_client
from my_utils import insert_into_mssql, load_my_columns, \
                        get_numbers,map_dff_to_my_columns, max_number_find

start = time()
write_to_table='bronze.katm_077_dynamics_of_scoring_ball'
columns_file = 'katm_077_dynamics_of_scoring_ball_fields.txt'
columns = load_my_columns(columns_file)
# max_num = max_number_find(write_to_table)

client = client = get_mongo_client()
db = client['task']
task_collection = db['task']
query = {
    'data.katm_077.return.data.dynamics_of_scoring_ball.periods': {'$exists': True}
    # ,'number': {'$eq': 7090166}
    # ,'number': {'$gt': max_num}
    # ,'number': {'$gt': 5000000, '$lt': 5001000}
    # ,'number': {'$in': numbers}
    
}
projection = {
    'number': 1,
    'data.katm_077.return.data.dynamics_of_scoring_ball.periods': 1
}
docs = task_collection.find(query, projection)#.sort('number', -1)

for doc in docs:
    rows = []
    _id = str(doc.get('_id')) 
    number = doc.get('number')
    fields = doc.get('data', {}).get('katm_077', {}).get('return', {}).get('data', {}).get('dynamics_of_scoring_ball', {}).get('periods', [])
    if isinstance(fields, dict):
        fields = [fields]
    elif fields is None:
        fields = []
    for field in fields:
        row = {'_id': _id, 'number': number, **field}
        rows.append(row)

    df = pd.DataFrame(rows)
    final_df = map_dff_to_my_columns(df, columns)
    insert_into_mssql(final_df, write_to_table)
end = time()
print(f"script took {end-start} seconds")