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
    check_table_query = f"""select max(number) from RISKDB.dbo.katm_333_monthly_detail;
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
            uuid = loan.get('uuid')
            monthly_details = loan.get('monthly_details')

            if not monthly_details or monthly_details == '' or (isinstance(monthly_details, (list, dict)) and not monthly_details):
                continue  # Skip if monthly_details is empty or missing

            if isinstance(monthly_details, dict):
                monthly_detail = monthly_details.get('monthly_detail')
                if isinstance(monthly_detail, dict):
                    row = {
                        'number': number,
                        'uuid': uuid,
                        'client_id': client_id,
                        'date': monthly_detail.get('date', ''),
                        'history_date': monthly_detail.get('history_date', ''),
                        'status': monthly_detail.get('status', ''),
                        'worst_status': monthly_detail.get('worst_status', ''),
                        'pmt_pat': monthly_detail.get('pmt_pat', ''),
                        'delq_balance': monthly_detail.get('delq_balance', ''),
                        'max_delq_balance': monthly_detail.get('max_delq_balance', ''),
                        'next_pmt': monthly_detail.get('next_pmt', ''),
                        'outstanding': monthly_detail.get('outstanding', ''),
                        'balance_amt': monthly_detail.get('balance_amt', '')
                    }
                    data.append(row)
                elif isinstance(monthly_detail, list):
                    for my_item in monthly_detail:
                        monthly_detail = my_item.get('monthly_detail')
                        if isinstance(monthly_detail, dict):
                            row = {
                                'number': number,
                                'uuid': uuid,
                                'client_id': client_id,
                                'date': my_item.get('date', ''),
                                'history_date': my_item.get('history_date', ''),
                                'status': my_item.get('status', ''),
                                'worst_status': my_item.get('worst_status', ''),
                                'pmt_pat': my_item.get('pmt_pat', ''),
                                'delq_balance': my_item.get('delq_balance', ''),
                                'max_delq_balance': my_item.get('max_delq_balance', ''),
                                'next_pmt': my_item.get('next_pmt', ''),
                                'outstanding': my_item.get('outstanding', ''),
                                'balance_amt': my_item.get('balance_amt', '')
                            }
                            data.append(row)
            elif isinstance(monthly_details, list):
                for monthly_details_item in monthly_details:
                    monthly_detail = monthly_details_item.get('monthly_detail')
                    if isinstance(monthly_detail, dict):
                        row = {
                            'number': number,
                            'uuid': uuid,
                            'client_id': client_id,
                            'date': monthly_details_item.get('date', ''),
                            'history_date': monthly_details_item.get('history_date', ''),
                            'status': monthly_details_item.get('status', ''),
                            'worst_status': monthly_details_item.get('worst_status', ''),
                            'pmt_pat': monthly_details_item.get('pmt_pat', ''),
                            'delq_balance': monthly_details_item.get('delq_balance', ''),
                            'max_delq_balance': monthly_details_item.get('max_delq_balance', ''),
                            'next_pmt': monthly_details_item.get('next_pmt', ''),
                            'outstanding': monthly_details_item.get('outstanding', ''),
                            'balance_amt': monthly_details_item.get('balance_amt', '')
                        }
                        data.append(row)
                    elif isinstance(monthly_detail, list):
                        for item in monthly_detail:
                            row = {
                                'number': number,
                                'uuid': uuid,
                                'client_id': client_id,
                                'date': item.get('date', ''),
                                'history_date': item.get('history_date', ''),
                                'status': item.get('status', ''),
                                'worst_status': item.get('worst_status', ''),
                                'pmt_pat': item.get('pmt_pat', ''),
                                'delq_balance': item.get('delq_balance', ''),
                                'max_delq_balance': item.get('max_delq_balance', ''),
                                'next_pmt': item.get('next_pmt', ''),
                                'outstanding': item.get('outstanding', ''),
                                'balance_amt': item.get('balance_amt', '')
                            }
                            data.append(row)

        # rows.append(row)
            
    if data:
        df = pd.DataFrame(data)
        insert_into_mssql(df, 'RISKDB.dbo.katm_333_monthly_detail')
            