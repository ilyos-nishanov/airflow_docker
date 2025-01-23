import json
from time import time
from threading import Thread
import logging
from connections import get_mongo_client
from my_utils import setup_table, load_my_columns, \
    max_number_find_with_schema, process_document_batch_with_logging

CONFIG_FILE = "mongodb_to_sql_config.json"

logging.basicConfig(
    filename='error_log.txt',
    level=logging.ERROR,  
    format='%(asctime)s - %(levelname)s - %(message)s'  
)

def main(doc_limit=None, batch_size=100):
    start = time()

    with open(CONFIG_FILE, "r") as config_file:
        config = json.load(config_file)

    client = get_mongo_client()
    db = client['task']
    task_collection = db['task']

    for entry in config:
        print(f"Processing: {entry['name']}")
        nums_table = entry['nums_table']
        write_to_table = entry["write_table"]
        columns_file = entry["columns_file"]
        query_path = entry["query_path"]
        projection_path = entry["projection_path"]
        field_path = entry["query_path"]

        columns = load_my_columns(columns_file)
        max_num = max_number_find_with_schema(nums_table)

        query = {
            query_path: {'$exists': True},
            'number': {'$gt': max_num}
        }
        projection = {
            'number': 1,
            projection_path: 1
        }
        docs_cursor = task_collection.find(query, projection)
        if doc_limit:
            docs_cursor = docs_cursor.limit(doc_limit)
        docs = list(docs_cursor)
        doc_batches = [docs[i:i + batch_size] for i in range(0, len(docs), batch_size)]

        # Create SQL table
        setup_table(write_to_table, columns)

        # Process batches using multithreading, writing to SQL right away
        threads = []
        for batch in doc_batches:
            t = Thread(target=process_document_batch_with_logging, args=(batch, field_path, columns, write_to_table))
            t.start()
            threads.append(t)

        # Ensure all threads start without waiting for their completion
        for t in threads:
            t.join()

    end = time()
    print(f"Processed {doc_limit if doc_limit else len(docs)} documents in batches of {batch_size}.")
    print(f"Script completed in {end - start:.2f} seconds")

if __name__ == "__main__":
    main(doc_limit=None, batch_size=100)
