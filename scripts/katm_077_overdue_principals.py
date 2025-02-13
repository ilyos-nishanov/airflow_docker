import pandas as pd
from katm_connections import get_mongo_client
from katm_utils import insert_into_mssql, load_my_columns\
                            ,map_dff_to_my_columns, max_number_find

table_name='bronze.katm_077_overdue_principals'
columns_file = 'katm_077_overdue_procents_fields.txt'
columns = load_my_columns(columns_file)
max_num = max_number_find(table_name)
print(max_num)

client = get_mongo_client()
db = client['task']
task_collection = db['task']
query = {
    'data.katm_077.return.data.contracts.contract': {
        '$elemMatch': {
            'overdue_principals.overdue_principal': {'$exists': True}
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
        overdue_principals = contract.get('overdue_principals')
        if isinstance(overdue_principals, dict):
            overdue_principal = overdue_principals.get('overdue_principal')
            if isinstance(overdue_principal, dict):
                overdue_principal = [overdue_principal]
            elif overdue_principal is None:
                overdue_principal = []
            for i in range(len(overdue_principal)):
                row = {'contract_id': contract_id, 'number': number, **overdue_principal[i]}
                rows.append(row)
    df = pd.DataFrame(rows)
    final_df = map_dff_to_my_columns(df, columns)
    insert_into_mssql(final_df, table_name)
