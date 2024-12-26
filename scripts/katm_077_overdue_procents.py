import pandas as pd
from connections import get_mongo_client
from my_utils import insert_into_mssql, load_my_columns\
                            ,map_dff_to_my_columns, max_number_find

table_name='bronze.katm_077_overdue_procents'
columns_file = 'katm_077_overdue_procents_fields.txt'
columns = load_my_columns(columns_file)
max_num = max_number_find(table_name)

client = get_mongo_client()
db = client['task']
task_collection = db['task']
query = {
    'data.katm_077.return.data.contracts.contract': {
        '$elemMatch': {
            'overdue_procents.overdue_procent': {'$exists': True}
        }
    }
    # ,'number': {'$eq': 4695757}
    ,'number': {'$gt': max_num}
    # ,'number': {'$gt': 4695757, '$lt': 4695900}
}
projection = {
    'number': 1,
    'data.katm_077.return.data.contracts.contract': 1
}
docs = task_collection.find(query,projection)
for doc in docs:
    rows = []
    number = doc.get('number')
    contracts_list = doc.get('data', {}).get('katm_077', {}).get('return', {}).get('data', {}).get('contracts', {}).get('contract', [])
    if isinstance(contracts_list, dict):
        contracts_list = [contracts_list]
    for contract in contracts_list:
        contract_id = contract.get('contract_id')
        overdue_procents = contract.get('overdue_procents')
        if isinstance(overdue_procents, dict):
            overdue_procent = overdue_procents.get('overdue_procent')
            if isinstance(overdue_procent, dict):
                overdue_procent = [overdue_procent]
            elif overdue_procent is None:
                overdue_procent = []
            for i in range(len(overdue_procent)):
                row = {'contract_id': contract_id, 'number': number, **overdue_procent[i]}
                rows.append(row)
    df = pd.DataFrame(rows)
    final_df = map_dff_to_my_columns(df, columns)
    insert_into_mssql(final_df, table_name)
