import json
import pandas as pd
from time import time
from connections import get_mongo_client
from my_utils import insert_into_mssql_with_schema, \
    load_my_columns, map_dff_to_my_columns, max_number_find_with_schema, \
    resolve_nested_field, setup_table

CONFIG_FILE = "mongodb_to_sql_config.json"

start = time()

with open(CONFIG_FILE, "r") as config_file:
    config = json.load(config_file)

client = get_mongo_client()
db = client['task']
task_collection = db['task']

for entry in config:
    print(f"Processing : {entry['name']}")
    nums_table = entry['nums_table']
    write_to_table = entry["write_table"]
    columns_file = entry["columns_file"]
    query_path = entry["query_path"]
    projection_path = entry["projection_path"]
    field_path = entry["query_path"]

    columns = load_my_columns(columns_file)
    max_num = max_number_find_with_schema(nums_table)
    print(max_num)
    setup_table(write_to_table, columns)

    query = {
        query_path: {'$exists': True},
        'number': {'$gt': max_num}
    }
    projection = {
        'number': 1,
        projection_path: 1
    }
    docs_cursor = task_collection.find(query, projection)

    for doc in docs_cursor:                 #   <<<<< switch between docs and limited_docs
        rows = []
        _id = str(doc.get('_id'))
        number = doc.get('number')
        fields = resolve_nested_field(doc, field_path)
        if isinstance(fields, dict):
            fields = [fields]
        for field in fields:
            row = {'_id': _id, 'number': number, **field}
            rows.append(row)
        if rows:
            df = pd.DataFrame(rows)
            final_df = map_dff_to_my_columns(df, columns)
            insert_into_mssql_with_schema(final_df, write_to_table)
            end = time()

end = time()
print(f"Script completed in {end - start:.2f} seconds")

