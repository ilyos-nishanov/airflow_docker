import json
import pandas as pd
from time import time, sleep
from datetime import datetime
from pymongo.errors import CursorNotFound
from katm_connections import get_mongo_client
from katm_utils import insert_into_mssql_tqdm

write_to_table = 'bronze.hints'
date_filter = datetime(2023, 11, 1)
query = {
    'data.katm_077.result': {'$exists': True}
    ,"created": {"$gt": date_filter}
}
projection = {
    'number': 1,
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
                    insert_into_mssql_tqdm(df, write_to_table)
                    rows = []
                i += 1
                number = doc.get('number')
                hint=doc.get('data', {}).get('katm_077', {}).get('result', {}).get('hint')
                row = {'number': number, 'hint':hint, }
                rows.append(row)
            df = pd.DataFrame(rows)
            if not df.empty:
                insert_into_mssql_tqdm(df, write_to_table)
            end = time()
            print(f"Script completed in {end - start} seconds")
            break
        except CursorNotFound:
            print("CursorNotFound error encountered. Restarting query...")
            sleep(5)
process_documents()
