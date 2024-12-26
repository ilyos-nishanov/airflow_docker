from pymongo import MongoClient
from datetime import datetime
import pandas as pd
from pandas import json_normalize
import pyodbc
from pymongo import MongoClient
from datetime import datetime
import pyodbc
from bson import ObjectId
from pymongo import MongoClient
import pandas as pd

def insert_into_mssql(df, table_name):
    # Connection details
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

    # Define mapping of Pandas dtypes to ODBC Driver 17 for SQL Server types
    dtype_mapping = {
        'int64': 'BIGINT',
        'float64': 'FLOAT',
        'datetime64[ns]': 'DATETIME',
        'bool': 'BIT',
        'object': 'NVARCHAR(1000)',
        'string': 'NVARCHAR(1000)'
    }

    # Generate SQL column definitions with types based on the DataFrame’s dtypes
    sql_columns = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        sql_type = dtype_mapping.get(dtype, 'NVARCHAR(1000)')  # Default to NVARCHAR(1000) if type not found
        sql_columns.append(f'[{col}] {sql_type}')

    # Check if table exists, if not create it with appropriate types
    check_table_query = f"""
    IF OBJECT_ID(N'{table_name}', 'U') IS NULL
    BEGIN
        CREATE TABLE {table_name} (
            {', '.join(sql_columns)}
        );
    END;"""
    cursor.execute(check_table_query)
    conn.commit()

    # Clean the DataFrame for ODBC Driver 17 for SQL Server compatibility
    df = df.fillna(value=pd.NA)  # Replace NaN/None with SQL-friendly NULL
    for col in df.select_dtypes(include=['float64']).columns:
        df[col] = df[col].round(4)  # Round floats to avoid excessive precision errors

    # Convert ObjectId to strings if present
    for col in df.columns:
        if df[col].dtype == 'object' and df[col].apply(lambda x: isinstance(x, ObjectId)).any():
            df[col] = df[col].astype(str)

    # Insert the data from DataFrame into the table
    for index, row in df.iterrows():
        try:
            insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in range(len(df.columns))])})"
            values = tuple(row.replace({pd.NA: None}))  # Replace pd.NA with None for SQL NULL handling
            cursor.execute(insert_query, values)
        except Exception as e:
            print(f"Error inserting row {index}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
# insert_into_mssql( df , 'salary_invoice_asbt')

# from pymongo import MongoClient
# import pandas as pd
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
    check_table_query = f"""select max(number) from RISKDB.dbo.prototype_overdue_principals_katm077;
    """
    cursor.execute(check_table_query)

    # Fetch the result (the single number)
    result = cursor.fetchone()

    # result will be a tuple with one element, so you can extract the number like this:
    max_number = int(result[0] if result else None)
    return max_number

# max_num = max_number_find()
# print(max_num)
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

# Define the query to fetch the first document where `overdue_procent` exists within any contract's `overdue_procents`
query = {
    'data.katm_077.return.data.contracts.contract': {
        '$elemMatch': {
            'overdue_principals.overdue_principal': {'$exists': True}
        }
    },
    # 'number': {'$gt': max_num} # max_date= 'select max(number) from table_name'
}

# Adjusted projection to include the entire contracts list
projection = {
    'number': 1,
    'data.katm_077.return.data.contracts.contract': 1
}

# Fetch the first document that matches the query
document = task_collection.find(query, projection)
print("connection to mongodb succcessful")

# Process the documents and handle cases with missing or various formats of `overdue_procent`
# data = []

for doc in document:
    data = []
    if doc:
        number = doc.get('number')
        contracts_list = doc.get('data', {}).get('katm_077', {}).get('return', {}).get('data', {}).get('contracts', {}).get('contract', [])

        # Ensure contracts_list is a list (it might be a dictionary if it's not an actual list of contracts)
        if isinstance(contracts_list, dict):
            contracts_list = [contracts_list]

        # Iterate over each contract in the contracts list
        for contract in contracts_list:
            contract_id = contract.get('contract_id')
            
            # Check if `overdue_procents` is a dictionary or a list of dictionaries
            overdue_procents = contract.get('overdue_principals')
            # overdue_procents = contract.get('overdue_principals')
            if not overdue_procents or overdue_procents == '' or (isinstance(overdue_procents, (list, dict)) and not overdue_procents):
                # Skip if overdue_principals is empty, missing, or an empty string
                continue
            if isinstance(overdue_procents, dict):
                # If overdue_procents is a single dictionary
                overdue_procent = overdue_procents.get('overdue_principal')
                if isinstance(overdue_procent, dict):
                    # Single dictionary case for overdue_procent
                    row = {
                        'number': number,
                        'contract_id': contract_id,
                        'overdue_principal_sum': overdue_procent.get('overdue_principal_sum', ''),
                        'overdue_date': overdue_procent.get('overdue_date', ''),
                        'overdue_principal_change': overdue_procent.get('overdue_principal_change', ''),
                        'overdue_principal_days': overdue_procent.get('overdue_principal_days', '')
                    }
                    data.append(row)
                elif isinstance(overdue_procent, list):
                    # List of dictionaries case for overdue_procent
                    for overdue in overdue_procent:
                        row = {
                            'number': number,
                            'contract_id': contract_id,
                            'overdue_principal_sum': overdue.get('overdue_principal_sum', ''),
                            'overdue_date': overdue.get('overdue_date', ''),
                            'overdue_principal_change': overdue.get('overdue_principal_change', ''),
                            'overdue_principal_days': overdue.get('overdue_principal_days', '')
                        }
                        data.append(row)
            
            elif isinstance(overdue_procents, list):
                # If overdue_procents is a list of dictionaries
                for overdue_procents_item in overdue_procents:
                    overdue_procent = overdue_procents_item.get('overdue_principal')
                    if isinstance(overdue_procent, dict):
                        # Single dictionary case for overdue_procent within the list
                        row = {
                            'number': number,
                            'contract_id': contract_id,
                            'overdue_principal_sum': overdue_procent.get('overdue_principal_sum', ''),
                            'overdue_date': overdue_procent.get('overdue_date', ''),
                            'overdue_principal_change': overdue_procent.get('overdue_principal_change', ''),
                            'overdue_principal_days': overdue_procent.get('overdue_principal_days', '')
                        }
                        data.append(row)
                    elif isinstance(overdue_procent, list):
                        # List of dictionaries case for overdue_procent within the list
                        for overdue in overdue_procent:
                            row = {
                                'number': number,
                                'contract_id': contract_id,
                                'overdue_principal_sum': overdue.get('overdue_principal_sum', ''),
                                'overdue_date': overdue.get('overdue_date', ''),
                                'overdue_principal_change': overdue.get('overdue_principal_change', ''),
                                'overdue_principal_days': overdue.get('overdue_principal_days', '')
                            }
                            data.append(row)

    # # Convert the data to a DataFrame
    # df = pd.DataFrame(data)
    # insert_into_mssql( df , 'overdue_principals_katm077')
    
     # Only proceed with insertion if data is not empty
    if data:
        df = pd.DataFrame(data)
        
        # Attempt insertion and handle SQL errors to skip problematic documents
        try:
            insert_into_mssql(df, 'bronze.katm_077_overdue_principals')
        except Exception as e:
            # print(f"Error during insertion for document number {number}: {e}")
            continue
    # else:
    #     print(f"No valid overdue principals found for document number {doc.get('number', 'unknown')} - skipping insertion.")
    
    
