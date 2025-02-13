
import logging
from time import time
from katm_connections import get_mongo_client

from katm_utils import (
    setup_table,
    load_my_columns,
    process_document,
    max_number_find_with_schema,
    fetch_all_documents_with_retry
)

start = time()
logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
num_table = 'katm_077_scoring'
write_to_table = 'katm_077_scoring'
columns_file = 'katm_077_scoring_fields.txt'
columns = load_my_columns(columns_file)
setup_table(write_to_table, columns, 'bronze')
max_num = max_number_find_with_schema(num_table, 'bronze')
print(max_num)

query = f"""
        
                select l.number
                from bronze.katm_077_overview l
                left join bronze.katm_077_scoring r
                on l.number=r.number
                where r.number is null

    """
# numbers = get_numbers_2(query)
# numbers = [int(i) for i in numbers]
# print(len(numbers))

client = get_mongo_client()
db = client['task']
task_collection = db['task']

query = {
    'data.katm_077.return.data.scorring': {'$exists': True},
    'number': {'$gt': max_num}         # Only fetch numbers greater than max_num
    # ,'number': {'$in': numbers}     
}
projection = {
    'number': 1,
    'data.katm_077.return.data.scorring': 1
}
field_path = "data.katm_077.return.data.scorring"

try:
    # Fetch all documents with retry and reconnection logic
    docs = fetch_all_documents_with_retry(task_collection, query, projection)
    
    # Process each document
    for doc in docs:
        process_document(doc, field_path, columns, write_to_table)

except RuntimeError as e:
    logging.error(f"Script failed: {e}")
except Exception as e:
    logging.error(f"Unexpected error: {e}")

print(f"All processes completed in {(time() - start)/60:.2f} minutes")