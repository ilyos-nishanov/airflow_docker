import pandas as pd
from katm_connections import get_mongo_client
from katm_utils import insert_into_mssql, load_my_columns\
                            ,map_dff_to_my_columns, max_number_find

table_name='bronze.katm_077_open_contracts'
columns_file = 'katm_077_open_contracts_columns.txt'
columns = load_my_columns(columns_file)
max_num = max_number_find(table_name)

client = get_mongo_client()
db = client['task']
task_collection = db['task']
query = {
    'data.katm_077.return.data.open_contracts.open_contract': {'$exists': True}
    # ,'number': {'$eq': 1243165}
    ,'number': {'$gt': max_num}
    # ,'number': {'$gt': 1243100, '$lt': 1243200}
}
projection = {
    'number': 1,
    'data.katm_077.return.data.open_contracts.open_contract': 1
}
docs = task_collection.find(query, projection)


for doc in docs:
    rows=[]
    _id = str(doc.get('_id')) 
    number = doc.get('number')
    fields = doc.get('data', {}).get('katm_077', {}).get('return', {}).get('data', {}).get('open_contracts', {}).get('open_contract', [])
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
    insert_into_mssql(final_df, table_name)