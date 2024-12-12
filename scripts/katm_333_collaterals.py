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
    check_table_query = f"""select max(number) from RISKDB.dbo.katm_333_collaterals;
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
docs = task_collection.find(query, projection)#.sort('number', -1).limit(1)
#document = list(docs)[0]

for doc in docs :
    data = []
    if doc:
        number = doc.get('number')
        client_id = doc.get('request', {}).get('clientId', {})
        loans = doc.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans', {}).get('loan', [])       

        if isinstance(loans, dict):
            loans = [loans]
        
        for loan in loans:
            
            uuid = loan.get('uuid')
            collaterals = loan.get('collaterals')
            
            if not collaterals or collaterals == '' or (isinstance(collaterals, (list, dict)) and not collaterals):
                # Skip if overdue_principals is empty, missing, or an empty string
                continue
            if isinstance(collaterals, dict):
                # If collaterals is a single dictionary
                collateral = collaterals.get('collateral')
                if isinstance(collateral, dict):
                    # Single dictionary case for collateral
                    row = {
                        'number': number,
                                'uuid': uuid,
                                'client_id': client_id,
                                'collateral_code': collateral.get('collateral_code', ''),
                                'collateral_id': collateral.get('collateral_id', ''),
                                'open_date': collateral.get('open_date', ''),
                                'value': collateral.get('value', ''),
                                'currency': collateral.get('currency', ''),
                                'end_date': collateral.get('end_date', ''),
                                'fact_end_date': collateral.get('fact_end_date', '')


                    }
                    data.append(row)
                elif isinstance(collateral, list):
                    # List of dictionaries case for collateral
                    for item in collateral:
                        my_item= item.get('collateral')
                        if isinstance(item, dict):
                            row = {
                                'number': number,
                                'uuid': uuid,
                                'client_id': client_id,
                                'collateral_code': item.get('collateral_code', ''),
                                'collateral_id': item.get('collateral_id', ''),
                                'open_date': item.get('open_date', ''),
                                'value': item.get('value', ''),
                                'currency': item.get('currency', ''),
                                'end_date': item.get('end_date', ''),
                                'fact_end_date': item.get('fact_end_date', '')


                            }
                            data.append(row)
            
            elif isinstance(collaterals, list):
                # If collaterals is a list of dictionaries
                for collaterals_item in collaterals:
                    collateral = collaterals_item.get('collateral')
                    if isinstance(collateral, dict):
                        # Single dictionary case for collateral within the list
                        row = {
                            'number': number,
                                'uuid': uuid,
                                'client_id': client_id,
                                'collateral_code': collaterals_item.get('collateral_code', ''),
                                'collateral_id': collaterals_item.get('collateral_id', ''),
                                'open_date': collaterals_item.get('open_date', ''),
                                'value': collaterals_item.get('value', ''),
                                'currency': collaterals_item.get('currency', ''),
                                'end_date': collaterals_item.get('end_date', ''),
                                'fact_end_date': collaterals_item.get('fact_end_date', '')


                        }
                        data.append(row)
                    elif isinstance(collateral, list):
                        # List of dictionaries case for collateral within the list
                        for item in collateral:
                            row = {
                                'number': number,
                                'uuid': uuid,
                                'client_id': client_id,
                                'collateral_code': item.get('collateral_code', ''),
                                'collateral_id': item.get('collateral_id', ''),
                                'open_date': item.get('open_date', ''),
                                'value': item.get('value', ''),
                                'currency': item.get('currency', ''),
                                'end_date': item.get('end_date', ''),
                                'fact_end_date': item.get('fact_end_date', '')


                            }
                            data.append(row)
        # rows.append(row)
            
    if data:
        df = pd.DataFrame(data)
        insert_into_mssql(df, 'RISKDB.dbo.katm_333_collaterals')
                