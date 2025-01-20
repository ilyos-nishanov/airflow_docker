import json
import pandas as pd
from time import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from connections import get_mongo_client
from my_utils import setup_table, insert_into_mssql_3, load_my_columns, \
    map_dff_to_my_columns, max_number_find, \
    resolve_nested_field

CONFIG_FILE = "mongodb_to_sql_config.json"

def process_document_batch(doc_batch, field_path, columns, write_to_table):
    for doc in doc_batch:
        rows = []
        _id = str(doc.get('_id'))
        number = doc.get('number')
        fields = resolve_nested_field(doc, field_path)
        if isinstance(fields, dict):
            fields = [fields]
        elif fields is None:
            fields = []

        for field in fields:
            row = {'_id': _id, 'number': number, **field}
            rows.append(row)

        if rows:
            df = pd.DataFrame(rows)
            final_df = map_dff_to_my_columns(df, columns)
            insert_into_mssql_3(final_df, write_to_table)

def main(doc_limit=1000, batch_size=100):
    start = time()

    with open(CONFIG_FILE, "r") as config_file:
        config = json.load(config_file)

    client = get_mongo_client()
    db = client['task']
    task_collection = db['task']

    for entry in config:
        print(f"Processing entry: {entry['name']}")
        nums_table = entry['nums_table']
        write_to_table = entry["write_table"]
        columns_file = entry["columns_file"]
        query_path = entry["query_path"]
        projection_path = entry["projection_path"]
        field_path = entry["query_path"]

        columns = load_my_columns(columns_file)
        max_num = max_number_find(nums_table)

        query = {
            query_path: {'$exists': True},
            'number': {'$gt': max_num}
        }
        projection = {
            'number': 1,
            projection_path: 1
        }
        docs_cursor = task_collection.find(query, projection).limit(doc_limit)
        docs = list(docs_cursor)  # Convert cursor to a list for slicing
        doc_batches = [docs[i:i + batch_size] for i in range(0, len(docs), batch_size)]

        # create sql table
        setup_table(write_to_table, columns)
        # Use ThreadPoolExecutor to process document batches in parallel
        with ThreadPoolExecutor(max_workers=4) as executor:  # Adjust max_workers as needed
            futures = []
            for batch in doc_batches:
                futures.append(executor.submit(process_document_batch, batch, field_path, columns, write_to_table))

            # Wait for all threads to complete
            for future in as_completed(futures):
                try:
                    future.result()  # Raise exception if any thread failed
                except Exception as e:
                    print(f"Error processing batch: {e}")

    end = time()
    print(f"Processed {doc_limit} documents in batches of {batch_size}.")
    print(f"Script completed in {end - start:.2f} seconds")

if __name__ == "__main__":
    # Adjust `doc_limit` and `batch_size` as needed
    main(doc_limit=1000, batch_size=100)  # Process 1000 documents with batch size 100
    # main(doc_limit=10000, batch_size=1000)  # Uncomment to process 10000 documents with batch size 1000
