import pandas as pd
from connections import get_mongo_client, get_sql_server_connection
from scripts.my_utils import convert_date, load_columns, map_dff_to_columns, max_number_find

columns_file = 'katm_333_collaterals.txt'
table_name = 'bronze.katm_333_collaterals'

def insert_into_mssql(df, table_name=table_name):
    conn = get_sql_server_connection()
    cursor = conn.cursor()

    for index, row in df.iterrows():
        try:
            insert_query = f"""
            INSERT INTO {table_name}
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            values = (
                row['number'],
                row['uuid'],
                row['client_id'],
                row['collateral_code'],
                row['collateral_id'],
                convert_date(row['open_date']),
                row['value'],
                row['currency'],
                convert_date(row['end_date']),
                convert_date(row['fact_end_date'])
            )
            cursor.execute(insert_query, values)
        except Exception as e:
            print(f"Error inserting row {index}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

# max_num = max_number_find(table_name)

client = client = get_mongo_client()
db = client['task']
task_collection = db['task']
query = {
    'data.katm_333.return.data.general_cbr.loans.loan': {'$exists': True},
    # 'number': {'$gt': max_num}
}
projection = {
    'number': 1, 
    'request.clientId': 1,
    'data.katm_333.return.data.general_cbr.loans.loan': 1
}
docs = task_collection.find(query, projection)

for document in docs:
    rows = []

    number = document.get('number')
    client_id = document.get('request', {}).get('clientId', {})
    loans = document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans', {}).get('loan', [])       

    if isinstance(loans, dict):
        loans = [loans]

    for loan in loans:
        row = {'client_id': client_id, 'number': number, **loan}
        rows.append(row)
        
    df = pd.DataFrame(rows)
    
    columns = load_columns(columns_file)
    final_df = map_dff_to_columns(df, columns)
    insert_into_mssql(final_df)