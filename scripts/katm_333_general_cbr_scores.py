import pyodbc
import pandas as pd
from bson import ObjectId
from datetime import datetime
from pymongo import MongoClient

# Function to insert DataFrame into MSSQL in chunks
def insert_into_mssql(df, table_name):

    driver = 'SQL Server'
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
    check_table_query = f"""select max(number) from RISKDB.dbo.katm_333_general_cbr_scores;
    """
    cursor.execute(check_table_query)

    # Fetch the result (the single number)
    result = cursor.fetchone()

    # result will be a tuple with one element, so you can extract the number like this:
    max_number = int(result[0] if result else None)
    return max_number

max_num = max_number_find()

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
    'data.katm_333.return.data.general_cbr.scores.score': {'$exists': True}
    , 'number': {'$gt': max_num} 
}

# Define the projection
projection = {
    'number': 1, 
    'request.clientId': 1,
    'data.katm_333.return.data.general_cbr.scores.score': 1

}


# Fetch docs tree
docs = task_collection.find(query, projection)

for document in docs :
    rows = []
    if document:
        _id = str(document.get('_id'))  # Convert `_id` to string
        # number = document.get('number')  # Extract the `number` field
        # contracts = document.get('data', {}).get('katm_077', {}).get('return', {}).get('data', {}).get('contracts', {}).get('contract', [])
        
        number = document.get('number')
        client_id = document.get('request', {}).get('clientId', {})
        scores = document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('scores', {}).get('score', [])       

        if isinstance(scores, dict):
            scores = [scores]
        
        for score in scores:
            # Flatten the contract details and include the `number` field
            # row = {'number': number, **contract}
            row = {'client_id': client_id, 'number': number, **score}
            rows.append(row)
            
        # Create DataFrame
        df = pd.DataFrame(rows)

        # Function to create an empty DataFrame with specified columns
        def create_empty_df_with_columns(columns):
            return pd.DataFrame(columns=columns)

        # Function to load contract columns from a text file
        def load_score_columns(file_path):
            with open(file_path, 'r') as file:
                columns = file.read().splitlines()  # Read each line as a column name
            return columns
        # Function to map `dff` values to a DataFrame with specified columns
        def map_dff_to_score_columns(dff, score_columns):
            # Ensure '_id' and 'number' are in the score_columns list
            essential_columns = ['client_id', 'number']
            for col in essential_columns:
                if col not in score_columns:
                    score_columns.append(col)
                    
            # Create an empty DataFrame with the contract columns
            final_df = create_empty_df_with_columns(score_columns)
            
            # Fill in the columns of `final_df` with values from `dff`
            for col in final_df.columns:
                if col in dff.columns:
                    final_df[col] = dff[col]  # Copy the values from `dff`
                else:
                    # final_df[col] = None  # Set the column to None if not in `dff`
                    final_df.drop(columns=[col], inplace=True) 
            
            return final_df

        scores_columns_file = 'katm_333_general_cbr_scores_fields.txt'

        score_columns = load_score_columns(scores_columns_file)

        final_df = map_dff_to_score_columns(df, score_columns)
        insert_into_mssql(final_df, 'katm_333_general_cbr_scores')


    # break