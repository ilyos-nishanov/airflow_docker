import json
import pandas as pd
from time import time
from connections import get_mongo_client
from my_utils import insert_into_mssql, load_my_columns, \
    get_numbers, map_dff_to_my_columns, max_number_find, \
    resolve_nested_field


CONFIG_FILE = "mongodb_to_sql_config.json"

def main():
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

        docs = task_collection.find(query, projection)
        limited_docs = docs[:1000]
        for doc in limited_docs:
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
                insert_into_mssql(final_df, write_to_table)
                end = time()
                print(f"took {end - start:.2f} seconds")

    end = time()
    print(f"Script completed in {end - start:.2f} seconds")

if __name__ == "__main__":
    main()
