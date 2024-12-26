import pandas as pd
from connections import get_mongo_client
from my_utils import insert_into_mssql, load_my_columns\
                            , map_dff_to_my_columns, max_number_find

table_name='bronze.katm_077_contingent_liabilities'
columns_file = 'katm_077_contingent_liabilities_columns.txt'
columns = load_my_columns(columns_file)
max_num = max_number_find(table_name)

client = get_mongo_client()
db = client['task']
task_collection = db['task']
query = {
    'data.katm_077.return.data.contingent_liabilities.contingent_liability': {'$exists': True}
    # ,'number': {'$eq': 1243165}
    ,'number': {'$gt': max_num}
    # ,'number': {'$gt': 1243100, '$lt': 1243200}    
}
projection = {
    'number': 1,
    'data.katm_077.return.data.contingent_liabilities.contingent_liability': 1
}
docs = task_collection.find(query, projection)

for doc in docs:
    rows = []
    _id = str(doc.get('_id')) 
    number = doc.get('number') 
    fields = doc.get('data', {}).get('katm_077', {}).get('return', {}).get('data', {}).get('contingent_liabilities', {}).get('contingent_liability', [])
    if isinstance(fields, dict):
        fields = [fields]
    elif fields is None:
        fields = []
    for field in fields:
        # filtered_field = {k: v for k, v in field.items() if not isinstance(v, (dict, list))}
        row = {'_id': _id, 'number': number, **fields}
        rows.append(row)

    df = pd.DataFrame(rows)
    final_df = map_dff_to_my_columns(df, columns)
    insert_into_mssql(final_df, table_name)