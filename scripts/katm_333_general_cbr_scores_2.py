import pandas as pd
from connections import get_mongo_client, get_sql_server_connection
from scripts.my_utils import convert_date, load_columns, map_dff_to_columns, max_number_find

scores_columns_file = 'katm_333_general_cbr_scores_fields.txt'
table_name = 'gold.katm_333_general_cbr_scores'
def insert_into_mssql(df, table_name=table_name):
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    for index, row in df.iterrows():
        try:
        # Insert data into SQL Server
            insert_query = f"""
                INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?)

            """
            values = (
                row['client_id'],
                row['number'],
                row['CARD_ID'],
                row['SCORE_VALUE'],
                convert_date(row['SCOR_DATE']),
                row['SCOR_NAME']

            )
            cursor.execute(insert_query, values)
        except Exception as e:
            print(f"Error inserting row {index}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
#############################################################################################################################################
client = client = get_mongo_client()
db = client['task']
task_collection = db['task']
# max_num = max_number_find(table_name)
query = {
    'data.katm_333.return.data.general_cbr.scores.score': {'$exists': True}
    # , 'number': {'$gt': max_num} 
}

# Define the projection
projection = {
    'number': 1, 
    'request.clientId': 1,
    'data.katm_333.return.data.general_cbr.scores.score': 1

}
docs = task_collection.find(query, projection)
############################################################################################################################################
for document in docs:
    rows = []

    number = document.get('number')
    client_id = document.get('request', {}).get('clientId', {})
    scores = document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('scores', {}).get('score', [])           

    if isinstance(scores, dict):
        scores = [scores]

    for score in scores:
        row = {'client_id': client_id, 'number': number, **score}
        rows.append(row)
        
    df = pd.DataFrame(rows)
    
    score_columns = load_columns(scores_columns_file)
    final_df = map_dff_to_columns(df, score_columns)
    insert_into_mssql(final_df)