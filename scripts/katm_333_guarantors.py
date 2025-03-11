import json
import pyodbc
import pandas as pd
from bson import ObjectId
from datetime import datetime
from pymongo import MongoClient

# Function to insert DataFrame into MSSQL in chunks
def insert_into_mssql(df, table_name):

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
    check_table_query = f"""select max(number) from bronze.katm_333_guarantors;
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
    'data.katm_333.return.data.general_cbr.loans.loan': {'$exists': True}
    , 'number': {'$gt': max_num} 
}

# Define the projection
projection = {
    'number': 1, 
    'request.clientId': 1,
    'data.katm_333.return.data.general_cbr.loans.loan': 1

}


# Fetch a single docs
docs = task_collection.find(query, projection)

for doc in docs :
    data = []
    if doc:
        number = doc.get('number')
        client_id = doc.get('request', {}).get('clientId', {})
        loans = doc.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans', {}).get('loan', [])       

        if isinstance(loans, dict):
            loans = [loans]
        
        for loan in loans:
            
            uuid_loan = loan.get('uuid')
            guarantors = loan.get('guarantors')
            
            if not guarantors or guarantors == '' or (isinstance(guarantors, (list, dict)) and not guarantors):
                # Skip if overdue_principals is empty, missing, or an empty string
                continue
            if isinstance(guarantors, dict):
                # If guarantors is a single dictionary
                guarantor = guarantors.get('guarantor')
                if isinstance(guarantor, dict):
                    # Single dictionary case for guarantor
                    row = {
                        'number': number,
                        'uuid_loan': uuid_loan,
                        'client_id': client_id,
                        'uuid_gtr': guarantor.get('uuid', ''),
                        'sum': guarantor.get('sum', ''),
                        'currency': guarantor.get('currency', ''),
                        'open_date': guarantor.get('open_date', ''),
                        'end_date': guarantor.get('end_date', ''),
                        'fact_end_date': guarantor.get('fact_end_date', ''),


                    }
                    data.append(row)
                elif isinstance(guarantor, list):
                    # List of dictionaries case for guarantor
                    for item in guarantor:
                        my_item= item.get('guarantor')
                        if isinstance(item, dict):
                            row = {
                                'number': number,
                                'uuid_loan': uuid_loan,
                                'client_id': client_id,
                                'uuid_gtr': item.get('uuid', ''),
                                'sum': item.get('sum', ''),
                                'currency': item.get('currency', ''),
                                'open_date': item.get('open_date', ''),
                                'end_date': item.get('end_date', ''),
                                'fact_end_date': item.get('fact_end_date', ''),


                            }
                            data.append(row)
            
            elif isinstance(guarantors, list):
                # If guarantors is a list of dictionaries
                for guarantors_item in guarantors:
                    guarantor = guarantors_item.get('guarantor')
                    if isinstance(guarantor, dict):
                        # Single dictionary case for guarantor within the list
                        row = {
                            'number': number,
                            'uuid_loan': uuid_loan,
                            'client_id': client_id,
                            'uuid_gtr': guarantors_item.get('uuid', ''),
                            'sum': guarantors_item.get('sum', ''),
                            'currency': guarantors_item.get('currency', ''),
                            'open_date': guarantors_item.get('open_date', ''),
                            'end_date': guarantors_item.get('end_date', ''),
                            'fact_end_date': guarantors_item.get('fact_end_date', ''),


                        }
                        data.append(row)
                    elif isinstance(guarantor, list):
                        # List of dictionaries case for guarantor within the list
                        for item in guarantor:
                            row = {
                                'number': number,
                                'uuid_loan': uuid_loan,
                                'client_id': client_id,
                                'uuid_gtr': item.get('uuid', ''),
                                'sum': item.get('sum', ''),
                                'currency': item.get('currency', ''),
                                'open_date': item.get('open_date', ''),
                                'end_date': item.get('end_date', ''),
                                'fact_end_date': item.get('fact_end_date', ''),


                            }
                            data.append(row)
        # rows.append(row)
            
    if data:
        df = pd.DataFrame(data)
        insert_into_mssql(df, 'bronze.katm_333_guarantors')
                