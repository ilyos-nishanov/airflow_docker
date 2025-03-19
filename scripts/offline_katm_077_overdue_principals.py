import pandas as pd
from datetime import datetime, timedelta
from katm_connections import get_mongo_client
from katm_utils import insert_into_mssql_tqdm, load_my_columns, \
                        map_dff_to_my_columns

start=datetime.now()
write_to_table = 'bronze.katm_077_overdue_principals_offline'
columns_file = 'katm_077_overdue_procents_fields.txt'
columns = load_my_columns(columns_file)

client = get_mongo_client()
db = client['katm']
collection = db['katm_cache']

query = {
    'reports.077.contracts.contract.overdue_procents.overdue_procent': {'$exists': True}
  }
projection = {
    '_id': 1,
    'reports.077.contracts.contract': 1
}

docs = collection.find(query, projection)

rows = []
batch_size = 100_000
for i, doc in enumerate(docs, 1):
    number = doc.get('number')
    _id = doc.get('_id')
    fields = doc.get('reports', {}).get('077', {}).get('contracts', {}).get('contract', [])
    if isinstance(fields, dict):
        fields = [fields]
    for contract in fields:
        contract_id = contract.get('contract_id')
        overdue_procents = contract.get('overdue_procents')
        if isinstance(overdue_procents, dict):
            overdue_procent = overdue_procents.get('overdue_procent')
            if isinstance(overdue_procent, dict):
                overdue_procent = [overdue_procent]
            elif overdue_procent is None:
                overdue_procent = []
            for i in range(len(overdue_procent)):
                row = {"_id":_id, 'contract_id': contract_id, 'number': number, **overdue_procent[i]}
                rows.append(row)

    if i % batch_size == 0:
        df = pd.DataFrame(rows)
        final_df = map_dff_to_my_columns(df, columns)
        if not df.empty:
            insert_into_mssql_tqdm(final_df, write_to_table)
        rows = []
if rows:
    df = pd.DataFrame(rows)
    final_df = map_dff_to_my_columns(df, columns)
    if not df.empty:
        insert_into_mssql_tqdm(final_df, write_to_table)

print(f"script took {datetime.now()-start}")