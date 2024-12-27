import json
import pandas as pd
from time import time
from connections import get_mongo_client
from my_utils import insert_into_mssql, load_my_columns, \
                                        get_numbers_2, map_dff_to_my_columns_2

start = time()

write_to_table = 'dbo.katm_333_general_cbr_loans'
columns_file = 'katm_333_general_cbr_loans_fields.txt'
columns = load_my_columns(columns_file)

query = f"""
        
                select o.number
                from dbo.katm_333_loans_overview o
                left join dbo.katm_333_general_cbr_loans l
                on o.number = l.number
                where l.number is null

    """
numbers = get_numbers_2(query)
numbers = [int(i) for i in numbers]
# numbers = [
#     7090120,
#     7090121,
#     7090122,
#     7090123,
#     7090124,
#     7090125,
#     7090126,
#     7090127,
#     7090128,
#     7090129
# ]

print(len(numbers))

client = client = get_mongo_client()
db = client['task']
task_collection = db['task']
query = {
    'data.katm_333.return.data.general_cbr.loans.loan': {'$exists': True}
    # ,'number': {'$eq': 7090123}
    # ,'number': {'$gt': max_num}
    # ,'number': {'$gt': 1243100, '$lt': 1243200}
    ,'number': {'$in': numbers}
    
}
projection = {
    'number': 1,
    'data.katm_333.return.data.general_cbr.loans.loan': 1
}
docs = task_collection.find(query, projection)#.sort('number', -1)
for doc in docs:
    rows = []
    number = doc.get('number')
    fields = doc.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans', {}).get('loan', [])
    if isinstance(fields, dict):
        fields = [fields]
    elif fields is None:
        fields = []
    for field in fields:
        row = {'number': number, **field}
        rows.append(row)

    df = pd.DataFrame(rows)
    final_df = map_dff_to_my_columns_2(df, columns)
    insert_into_mssql(final_df, write_to_table)
end = time()
print(final_df)
print(f"script took {end-start} seconds")