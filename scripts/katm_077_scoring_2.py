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

query = f"""
        
                select l.number
                from bronze.katm_077_overview l
                left join bronze.katm_077_scoring r
                on l.number=r.number
                where r.number is null

    """
numbers = get_numbers_2(query)
numbers = [int(i) for i in numbers]
print(len(numbers))

client = client = get_mongo_client()
db = client['task']
task_collection = db['task']
query = {
    'data.katm_077.return.data.scorring': {'$exists': True}
    # ,'number': {'$eq': 5651095}
    # ,'number': {'$gt': max_num}
    # ,'number': {'$gt': 1243100, '$lt': 1243200}
    ,'number': {'$in': numbers}
    
}
projection = {
    'number': 1,
    'data.katm_077.return.data.scorring': 1
}
docs = task_collection.find(query, projection)#.sort('number', -1)
for doc in docs:
    rows = []
    _id = str(doc.get('_id')) 
    number = doc.get('number')
    fields = doc.get('data', {}).get('katm_077', {}).get('return', {}).get('data', {}).get('scorring', {})
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