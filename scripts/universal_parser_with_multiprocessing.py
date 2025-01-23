import json
import logging
from time import time
from multiprocessing import Process
from connections import get_mongo_client

from my_utils import (
    load_my_columns,
    max_number_find_with_schema,
    setup_table,
    process_document,
    fetch_all_documents_with_retry
)

CONFIG_FILE = "mongodb_to_sql_config.json"


def process_entry(entry):
    logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
    print(f"Starting process for: {entry['name']}")

    client = get_mongo_client()
    db = client['task']
    task_collection = db['task']

    nums_table = entry['nums_table']
    write_to_table = entry["write_table"]
    columns_file = entry["columns_file"]
    query_path = entry["query_path"]
    projection_path = entry["projection_path"]
    field_path = entry["query_path"]

    columns = load_my_columns(columns_file)
    max_num = max_number_find_with_schema(nums_table)
    setup_table(write_to_table, columns)

    query = {
        query_path: {'$exists': True},
        'number': {'$gt': max_num}
    }
    projection = {
        'number': 1,
        projection_path: 1
    }
    try:
        docs = fetch_all_documents_with_retry(task_collection, query, projection)
        for doc in docs:
            process_document(doc, field_path, columns, write_to_table)

    except RuntimeError as e:
        logging.error(f"Script failed: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise

def main():
    start = time()
    with open(CONFIG_FILE, "r") as config_file:
        config = json.load(config_file)

    processes = []
    for entry in config:
        process = Process(target=process_entry, args=(entry,))
        processes.append(process)
        process.start()

    # Wait for all processes to complete
    for process in processes:
        process.join()

    end = time()
    print(f"All processes completed in {(end - start)/60:.2f} minutes")

if __name__ == "__main__":
    main()
