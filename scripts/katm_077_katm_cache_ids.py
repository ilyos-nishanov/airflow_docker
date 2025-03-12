import pandas as pd
from katm_connections import get_mongo_client
from katm_utils import insert_into_mssql_tqdm

write_to_table = 'bronze.katm_077_cache_ids'
client = get_mongo_client()
db = client['crm']
collection = db['action']

query = {
    'credit.katm_cache_id': {'$exists': True}
}

projection = {
    'number': 1,
    'credit.katm_cache_id': 1,
    '_id': 0 
}
docs = collection.find(query, projection)

rows = []
batch_size = 100_000
for i, doc in enumerate(docs, 1):
    number = doc.get('number')
    katm_cache_id = doc.get('credit', {}).get('katm_cache_id')
    
    if number is None or katm_cache_id is None:
        continue
    rows.append({'number': number, 'katm_cache_id': katm_cache_id})
    if i % batch_size == 0:
        df = pd.DataFrame(rows)
        if not df.empty:
            insert_into_mssql_tqdm(df, write_to_table)
        rows = []
if rows:
    df = pd.DataFrame(rows)
    if not df.empty:
        insert_into_mssql_tqdm(df, write_to_table)
