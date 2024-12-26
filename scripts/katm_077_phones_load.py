import os
import pandas as pd
from pymongo import MongoClient
from datetime import datetime
from pandas import json_normalize
import pyodbc
from bson import ObjectId

def max_number_find():
    driver = 'ODBC Driver 17 for SQL Server'
    server = '172.17.17.22,54312'
    database = 'RISKDB'
    username = 'SMaksudov'
    password = 'CfhljhVfrc#'

    conn = pyodbc.connect(
        f"Driver={{{driver}}};"
        f"Server={server};"
        f"Database={database};"
        f"UID={username};"
        f"PWD={password};"
    )

    cursor = conn.cursor()

    # Check if table exists, if not create it
    check_table_query = f"""select max(number) from RISKDB.dbo.phones_task_katm077;
    """
    cursor.execute(check_table_query)

    # Fetch the result (the single number)
    result = cursor.fetchone()

    # result will be a tuple with one element, so you can extract the number like this:
    max_number = int(result[0] if result else None)
    return max_number

# max_num = max_number_find()

# Function to insert DataFrame into MSSQL in chunks
def insert_into_mssql(df, table_name):
    # driver = 'SQL Server'
    # server = 'your_server_name'  # Replace with your server name
    # database = 'your_database_name'  # Replace with your database name
    # username = 'your_username'  # Replace with your username
    # password = 'your_password'  # Replace with your password
    driver = 'ODBC Driver 17 for SQL Server'
    server = '172.17.17.22,54312'
    database = 'RISKDB'
    username = 'SMaksudov'
    password = 'CfhljhVfrc#'
    
    conn = pyodbc.connect(
        f"Driver={{{driver}}};"
        f"Server={server};"
        f"Database={database};"
        f"UID={username};"
        f"PWD={password};"
    )

    cursor = conn.cursor()

    # Check if table exists, if not create it
    check_table_query = f"""
    IF OBJECT_ID(N'{table_name}', 'U') IS NULL
    BEGIN
        CREATE TABLE {table_name}  (
    {', '.join([f'[{col}] NVARCHAR(1000)' for col in df.columns])}
);
    END;"""
    cursor.execute(check_table_query)
    conn.commit()

    # Insert the data from DataFrame into the table
    for index, row in df.iterrows():
        try:
            insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in range(len(df.columns))])})"
            values = [str(val) for val in row]
            cursor.execute(insert_query, tuple(values))
        except Exception as e:
            print(f"Error inserting row {index}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
    
    
# Connect to MongoDB
client = MongoClient(
    'mongodb://172.17.39.13:27017',
    username='Sardor.Maksudov',
    password='jDS3pqTV',
    authSource='admin'
)

# Select the database and collection
db = client['task']
task_collection = db['task']
# max_date= 'select max(number) from table_name'

# Define the query
query = {
    'data.katm_077.return.data.client.phones.phone': {'$exists': True},
    # 'number': {'$gt': max_num} # max_date= 'select max(number) from table_name'
}

# Define the projection
projection = {
    'number': 1,
    'data.katm_077.return.data.client.phones.phone': 1
}


# Fetch a single document
docs = task_collection.find(query, projection)

# Normalize `docs` to always be a list of dictionaries
# if isinstance(docs, dict):
#     docs = [docs]  # Wrap a single dict into a list

# for document in docs :
#     rows = []
#     if document:
#         _id = str(document.get('_id'))  # Convert `_id` to string
#         number = document.get('number')  # Extract the `number` field
#         contracts = document.get('data', {}).get('katm_077', {}).get('return', {}).get('data', {}).get('client', {}).get('phones', {}).get('phone', [])
#         for contract in contracts:
#             # Flatten the contract details and include the `number` field
#             # row = {'number': number, **contract}
#             row = {'_id': _id, 'number': number, **contract}
#             rows.append(row)
            
            # Flatten the contract details and include `_id` and `number` fields
        
for document in docs:
    rows = []
    if document:
        # Extract fields from the document
        _id = str(document.get('_id'))  # Convert `_id` to string
        number = document.get('number')  # Extract the `number` field
        contracts = (
            document.get('data', {})
                    .get('katm_077', {})
                    .get('return', {})
                    .get('data', {})
                    .get('client', {})
                    .get('phones', {})
                    .get('phone', [])
        )

        # Ensure `contracts` is a list
        if not isinstance(contracts, list):
            contracts = [contracts]

        # Process each contract and flatten it
        for contract in contracts:
            if isinstance(contract, dict):  # Check if `contract` is a dictionary
                row = {'_id': _id, 'number': number, **contract}
            else:
                # If `contract` is not a dictionary, store it in a generic key
                row = {'_id': _id, 'number': number, 'contract': contract}
            rows.append(row)

    # Create DataFrame from the processed rows
    df = pd.DataFrame(rows)

    # # Create DataFrame
    # df = pd.DataFrame(rows)


    # Function to clean nested columns and prepare final DataFrame
    def clean_nested_columns(df):
        # Remove columns with lists or dictionaries
        for col in df.columns:
            if any(isinstance(val, (list, dict)) for val in df[col]):
                df = df.drop(columns=[col])
        return df


    # Function to write the last loaded index to the checkpoint file


    # Function to create an empty DataFrame with specified columns
    def create_empty_df_with_columns(columns):
        return pd.DataFrame(columns=columns)

    # Function to load contract columns from a text file
    def load_contract_columns(file_path):
        with open(file_path, 'r') as file:
            columns = file.read().splitlines()  # Read each line as a column name
        return columns
    # Function to map `dff` values to a DataFrame with specified columns
    def map_dff_to_contract_columns(dff, contract_columns):
        # Ensure '_id' and 'number' are in the contract_columns list
        essential_columns = ['_id', 'number']
        for col in essential_columns:
            if col not in contract_columns:
                contract_columns.append(col)
                
        # Create an empty DataFrame with the contract columns
        final_df = create_empty_df_with_columns(contract_columns)
        
        # Fill in the columns of `final_df` with values from `dff`
        for col in final_df.columns:
            if col in dff.columns:
                final_df[col] = dff[col]  # Copy the values from `dff`
            else:
                final_df[col] = None  # Set the column to None if not in `dff`
        
        return final_df
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    phones_columns_file = os.path.join(script_dir, 'katm_077_phones_columns.txt')

    phones_columns = load_contract_columns(phones_columns_file)

    final_df = map_dff_to_contract_columns(df, phones_columns)

    insert_into_mssql(final_df, 'bronze.katm_077_phone_numbers')



