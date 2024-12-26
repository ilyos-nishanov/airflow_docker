import pandas as pd
from connections import get_mongo_client
from my_utils import insert_into_mssql, load_columns\
                        ,max_number_find

table_name = 'bronze.katm_077_scoring_test'
columns_file = 'katm_077_scoring_columns.txt'
columns = load_columns(columns_file)
max_num = max_number_find(table_name)

client = client = get_mongo_client()
db = client['task']
task_collection = db['task']
query = {
    'data.katm_077': {'$exists': True}
    # ,'number': {'$gt': 6000000, '$lt': 6000100}
    , 'number': {'$gt': max_num}
    , 'number':{'$eq': 1345942}
}
projection = {
    '_id': 1,
    'number': 1,
    'data.katm_077.return.data.scorring.scoring_grade': 1,
    'data.katm_077.return.data.scorring.scoring_class': 1,
    'data.katm_077.return.data.scorring.scoring_level': 1,
    'data.katm_077.return.data.scorring.scoring_version': 1
}
docs = task_collection.find(query, projection)

for doc in docs:
    _id = str(doc.get('_id')) 
    number = doc.get('number')
    scoring_vals = doc.get('data', {}).get('katm_077', {}).get('return', {}).get('data', {}).get('scorring', {})
    row = {
        '_id': _id,
        'number': number,
        'scoring_version': scoring_vals.get('scoring_version'),
        'scoring_grade': scoring_vals.get('scoring_grade'),
        'scoring_class': scoring_vals.get('scoring_class'),
        'scoring_level': scoring_vals.get('scoring_level'),
    }
    df = pd.DataFrame(row, index=[0])
    insert_into_mssql(df, table_name)