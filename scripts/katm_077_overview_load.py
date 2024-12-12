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
    check_table_query = f"""select max(number) from katm_077_overview;
    """
    cursor.execute(check_table_query)

    # Fetch the result (the single number)
    result = cursor.fetchone()

    # result will be a tuple with one element, so you can extract the number like this:
    max_number = int(result[0] if result else None)
    return max_number

max_num = max_number_find()

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
    'data.katm_077.return.data.overview.claims_qty': {'$exists': True},
    'number': {'$gt': max_num} # max_date= 'select max(number) from table_name'
}

# Define the projection
projection = {
    '_id': 1 , 
    'number': 1, 
    'data.katm_077.return.data.overview.claims_qty': 1,
    'data.katm_077.return.data.overview.contracts_qty': 1,
    'data.katm_077.return.data.overview.contingent_liabilities_qty': 1,
    'data.katm_077.return.data.overview.credit_request_qty': 1,
    'data.katm_077.return.data.overview.subscriptions_qty': 1,
    'data.katm_077.return.data.overview.average_monthly_payment': 1,
    'data.katm_077.return.data.overview.overdue_principal_qty': 1,
    'data.katm_077.return.data.overview.max_overdue_principal_days': 1,
    'data.katm_077.return.data.overview.max_overdue_principal_sum': 1,
    'data.katm_077.return.data.overview.max_uninter_overdue_percent_days': 1,
    'data.katm_077.return.data.overview.total_overdue_percent_sum': 1,
    'data.katm_077.return.data.overview.actual_average_monthly_payment': 1
}


# Fetch a single document
docs = task_collection.find(query, projection)

for document in docs:
    data = {
        '_id': str(document.get('_id')),  # Convert `_id` to string
        'number': document.get('number'),
        'claims_qty': document.get('data', {}).get('katm_077', {}).get('return', {}).get('data', {}).get('overview', {}).get('claims_qty'),
        'contracts_qty': document.get('data', {}).get('katm_077', {}).get('return', {}).get('data', {}).get('overview', {}).get('contracts_qty'),
        'contingent_liabilities_qty': document.get('data', {}).get('katm_077', {}).get('return', {}).get('data', {}).get('overview', {}).get('contingent_liabilities_qty'),
        'credit_request_qty': document.get('data', {}).get('katm_077', {}).get('return', {}).get('data', {}).get('overview', {}).get('credit_request_qty'),
        'subscriptions_qty': document.get('data', {}).get('katm_077', {}).get('return', {}).get('data', {}).get('overview', {}).get('subscriptions_qty'),
        'average_monthly_payment': document.get('data', {}).get('katm_077', {}).get('return', {}).get('data', {}).get('overview', {}).get('average_monthly_payment'),
        'overdue_principal_qty': document.get('data', {}).get('katm_077', {}).get('return', {}).get('data', {}).get('overview', {}).get('overdue_principal_qty'),
        'max_overdue_principal_days': document.get('data', {}).get('katm_077', {}).get('return', {}).get('data', {}).get('overview', {}).get('max_overdue_principal_days'),
        'max_overdue_principal_sum': document.get('data', {}).get('katm_077', {}).get('return', {}).get('data', {}).get('overview', {}).get('max_overdue_principal_sum'),
        'max_uninter_overdue_percent_days': document.get('data', {}).get('katm_077', {}).get('return', {}).get('data', {}).get('overview', {}).get('max_uninter_overdue_percent_days'),
        'total_overdue_percent_sum': document.get('data', {}).get('katm_077', {}).get('return', {}).get('data', {}).get('overview', {}).get('total_overdue_percent_sum'),
        'actual_average_monthly_payment': document.get('data', {}).get('katm_077', {}).get('return', {}).get('data', {}).get('overview', {}).get('actual_average_monthly_payment'),
    }

    # Convert to pandas DataFrame
    df = pd.DataFrame([data])  # Wrap in a list to create a single-row DataFrame
    
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

    guarantees_columns_file = 'katm_077_overview_columns.txt'

    contract_columns = load_contract_columns(guarantees_columns_file)

    final_df = map_dff_to_contract_columns(df, contract_columns)

    insert_into_mssql(final_df, 'katm_077_overview')