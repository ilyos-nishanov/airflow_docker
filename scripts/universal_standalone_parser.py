import json
import logging
import pandas as pd
from time import time, sleep
from multiprocessing import Process
from pymongo.errors import CursorNotFound, PyMongoError
from bson.errors import InvalidBSON
from connections import get_mongo_client

from my_utils import (
    load_my_columns,
    max_number_find_with_schema,
    setup_table,
    resolve_nested_field,
    map_dff_to_my_columns,
    insert_into_mssql_with_schema
)

CONFIG_FILE = "mongodb_to_sql_config.json"

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_all_documents_with_retry(task_collection, query, projection, retries=3, delay=5):
    attempt = 0
    while attempt < retries:
        try:
            client = get_mongo_client()
            with client.start_session() as session:  # Explicitly manage the session
                # Ensure the session and cursor are used together
                cursor = task_collection.find(
                    query,
                    projection,
                    no_cursor_timeout=True,
                    session=session  # Pass the session explicitly
                )
                # Convert cursor to a list within the session to avoid session expiry
                documents = list(cursor)
                return documents  # Return the list of documents
            
        except CursorNotFound as e:
            attempt += 1
            logging.error(f"CursorNotFound error: {e}. Retrying {attempt}/{retries} in {delay} seconds...")
            sleep(delay)
        except PyMongoError as e:
            logging.error(f"Unexpected MongoDB error: {e}. Retrying {attempt}/{retries} in {delay} seconds...")
            attempt += 1
            sleep(delay)
    raise RuntimeError("Max retries reached. Unable to fetch documents from MongoDB.")

def process_entry(entry):
    """Process a single entry configuration."""
    logging.info(f"Starting process for: {entry['name']}")

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
    print(f"max_num for: {entry['name']}: {max_num}")
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
        doc_count = sum(1 for _ in docs)
        print(f"Fetched {doc_count} documents from MongoDB.")
        for doc in docs:
            process_document_batch_with_logging(doc, field_path, columns, write_to_table)

    except Exception as e:
        logging.error(f"Unexpected error while processing entry {entry['name']}: {e}")


def process_document_batch_with_logging(doc, field_path, columns, write_to_table):
    """Process a document batch with detailed logging."""
    try:
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
            insert_into_mssql_with_schema(final_df, write_to_table)

    except InvalidBSON as e:
        logging.error(f"Skipping document with _id: {_id} due to InvalidBSON error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error while processing document with _id: {_id}. Error: {e}")

def main():
    start = time()
    with open(CONFIG_FILE, "r") as config_file:
        config = json.load(config_file)

    processes = []
    for entry in config:
        process = Process(target=process_entry, args=(entry,))
        processes.append(process)
        process.start()

    for process in processes:
        process.join()

    end = time()
    logging.info(f"All processes completed in {(end - start)/60:.2f} minutes")

if __name__ == "__main__":
    main()
