import json
import pandas as pd
from time import time, sleep
from pymongo.errors import CursorNotFound
from katm_connections import get_mongo_client
from katm_utils import (
    insert_into_mssql, 
    load_my_columns, 
    # select,
    max_number_find,
    map_dff_to_my_columns_2
    )

start = time()
print(start)

write_to_table = 'bronze.katm_333_loans_overview'
columns_file = 'katm_333_loans_overview_fields.txt'
columns = load_my_columns(columns_file)

max_num = max_number_find(write_to_table)

# query = "SELECT number FROM bronze.katm_333_loans_overview"
# numbers = select(query)
# numbers = set(int(i) for i in numbers)
# numbers = list(numbers)
# numbers.sort()

client = get_mongo_client()
db = client['task']
task_collection = db['task']

query = {
    'data.katm_333.return.data.general_cbr.loans_overview': {'$exists': True},
    'number':{'$gt':max_num}
    # 'number': {'$nin': numbers} # to bring in newly updated old mongodb data. i.e. them old ones they brought in mongodb recentlyto
}
projection = {
    'number': 1,
    'request.clientId': 1,
    'data.katm_333.return.data.general_cbr.loans_overview': 1
}

processed_numbers = set()  # Track completed documents

def process_docs():
    global client, task_collection
    while True:
        try:
            docs = task_collection.find(query, projection).batch_size(1000)
            for doc in docs:
                number = doc.get('number')
                if number in processed_numbers:
                    continue  # Skip already processed docs

                rows = []
                id = doc.get('request', {}).get('clientId', {})
                fields = doc.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {})
                
                if isinstance(fields, dict):
                    fields = [fields]
                elif fields is None:
                    fields = []

                for field in fields:
                    row = {'id': id, 'number': number, **field}
                    rows.append(row)

                df = pd.DataFrame(rows)
                final_df = map_dff_to_my_columns_2(df, columns)
                insert_into_mssql(final_df, write_to_table)

                processed_numbers.add(number)  # Mark as processed
            
            break  # Exit loop when done

        except CursorNotFound:
            print("Cursor lost, reconnecting and resuming...")
            sleep(5)  # Wait before reconnecting
            client = get_mongo_client()  # Reconnect to MongoDB
            task_collection = client['task']['task']

process_docs()

end = time()
print(f"Script took {end-start} seconds")
