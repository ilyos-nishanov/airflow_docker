import time
import pandas as pd
from katm_connections import get_mongo_client
from katm_utils import insert_into_mssql, load_my_columns, map_dff_to_my_columns, max_number_find
import pymongo

def run_script():
    table_name = 'bronze.katm_077_overview'
    columns_file = 'katm_077_overview_columns.txt'
    columns = load_my_columns(columns_file)
    max_num = max_number_find(table_name)

    client = get_mongo_client()
    db = client['task']
    task_collection = db['task']
    query = {
        'data.katm_077.return.data.overview': {'$exists': True},
        'number': {'$gt': max_num}
    }
    projection = {
        'number': 1,
        'data.katm_077.return.data.overview': 1
    }

    try:
        docs = task_collection.find(query, projection)

        for doc in docs:
            rows = []
            _id = str(doc.get('_id'))
            number = doc.get('number')
            fields = doc.get('data', {}).get('katm_077', {}).get('return', {}).get('data', {}).get('overview', {})
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
                insert_into_mssql(final_df, table_name)
                print(f"Inserted {len(rows)} rows for document number: {number}")

    except pymongo.errors.CursorNotFound as e:
        print(f"CursorNotFound error: {e}. Restarting the script...")
        time.sleep(5)  # Optional delay before restarting
        run_script()  # Restart the script in case of cursor errors

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise  # Optionally raise the error to stop execution

# Start the script
run_script()
