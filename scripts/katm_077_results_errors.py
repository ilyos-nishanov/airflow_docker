import json
import pandas as pd
from time import time, sleep
from datetime import datetime
from pymongo.errors import CursorNotFound
from katm_connections import get_mongo_client
from katm_utils import insert_into_mssql_tqdm, load_my_columns, \
                        map_dff_to_my_columns, max_number_find_with_cast

write_to_table = 'bronze.katm_077_results_errors'
columns_file = 'katm_077_results_error_fields.txt'
columns = load_my_columns(columns_file)
max_num = max_number_find_with_cast(write_to_table)

date_filter = datetime(2023, 11, 1)

query = {
    'data.katm_077.result': {'$exists': True}
    ,'number': {'$gt': max_num}
    ,"created": {"$gt": date_filter}
    # ,"created":{"$lt": datetime(2023,12,15)}
}
projection = {
    'number': 1,
    'created': 1,
    'request.clientId': 1,
    'data.katm_077.result': 1
}

def process_documents():
    start = time()
    
    client = get_mongo_client()
    db = client['task']
    task_collection = db['task']
    
    while True:
        try:
            docs = task_collection.find(query, projection)
            i = 0
            rows = []

            for doc in docs:
                if i % 10_000 == 0 and rows:
                    df = pd.DataFrame(rows)
                    final_df = map_dff_to_my_columns(df, columns)
                    insert_into_mssql_tqdm(final_df, write_to_table)
                    rows = []

                i += 1
                number = doc.get('number')
                clientId = doc.get('request', {}).get('clientId')
                is_ok = doc.get('data', {}).get('katm_077', {}).get('result', {}).get('is_ok')
                hint=doc.get('data', {}).get('katm_077', {}).get('result', {}).get('hint')
                fields = doc.get('data', {}).get('katm_077', {}).get('result', {}).get('error', {})

                if isinstance(fields, dict):
                    fields = [fields]
                elif fields is None:
                    fields = []

                for field in fields:
                    row = {'number': number, 'clientId': clientId, 'is_ok': is_ok, 'hint':hint, **field}
                    rows.append(row)

            df = pd.DataFrame(rows)
            if not df.empty:
                final_df = map_dff_to_my_columns(df, columns)
                insert_into_mssql_tqdm(final_df, write_to_table)

            end = time()
            print(f"Script completed in {end - start} seconds")
            break

        except CursorNotFound:
            print("CursorNotFound error encountered. Restarting query...")
            sleep(5)

process_documents()
