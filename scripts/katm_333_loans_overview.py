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
    check_table_query = f"""select max(number) from RISKDB.dbo.katm_333_loans_overview;
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
    'data.katm_333.return.data.general_cbr.loans_overview': {'$exists': True}
    , 'number': {'$gt': max_num} 
}

# Define the projection
projection = {
    '_id': 1 , 
    'number': 1, 
    'request.clientId': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.inquiry_1_week': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.loans_coborrower': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.loans_main_borrower': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.inquiry_recent_period': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.first_loan_date': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.ttl_accounts': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.ttl_delq_60_89': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.inquiry_9_month': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.inquiry_12_month': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.ttl_delq_90_plus': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.inquiry_3_month': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.ttl_delq_30_l24m': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.loans_active': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.loans_active_coborrower': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.ttl_delq_30_59_l24m': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.max_overdue_status': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.inquiry_6_month': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.ttl_inquiries': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.ttl_delq_30': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.inquiry_1_month': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.ttl_delq_90_119_l24m': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.inquiry_1_day': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.ttl_delq_60_89_l24m': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.worst_status_ever': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.last_loan_date': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.ttl_delq_120_plus_l24m': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.pay_load': 1,
    'data.katm_333.return.data.general_cbr.loans_overview.ttl_delq_30_59': 1

}


# Fetch a single document
documents = task_collection.find(query, projection)

for document in documents:
    data = {
        'number': document.get('number'),
        'id': document.get('request', {}).get('clientId', {}),
        'inquiry_1_week': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('inquiry_1_week'),
        'loans_coborrower': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('loans_coborrower'),
        'loans_main_borrower': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('loans_main_borrower'),
        'inquiry_recent_period': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('inquiry_recent_period'),
        'first_loan_date': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('first_loan_date'),
        'ttl_accounts': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('ttl_accounts'),
        'ttl_delq_60_89': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('ttl_delq_60_89'),
        'inquiry_9_month': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('inquiry_9_month'),
        'inquiry_12_month': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('inquiry_12_month'),
        'ttl_delq_90_plus': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('ttl_delq_90_plus'),
        'inquiry_3_month': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('inquiry_3_month'),
        'ttl_delq_30_l24m': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('ttl_delq_30_l24m'),
        'loans_active': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('loans_active'),
        'loans_active_coborrower': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('loans_active_coborrower'),
        'ttl_delq_30_59_l24m': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('ttl_delq_30_59_l24m'),
        'max_overdue_status': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('max_overdue_status'),
        'inquiry_6_month': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('inquiry_6_month'),
        'ttl_inquiries': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('ttl_inquiries'),
        'ttl_delq_30': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('ttl_delq_30'),
        'inquiry_1_month': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('inquiry_1_month'),
        'ttl_delq_90_119_l24m': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('ttl_delq_90_119_l24m'),
        'inquiry_1_day': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('inquiry_1_day'),
        'ttl_delq_60_89_l24m': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('ttl_delq_60_89_l24m'),
        'worst_status_ever': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('worst_status_ever'),
        'last_loan_date': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('last_loan_date'),
        'ttl_delq_120_plus_l24m': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('ttl_delq_120_plus_l24m'),
        'pay_load': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('pay_load'),
        'ttl_delq_30_59': document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans_overview', {}).get('ttl_delq_30_59'),
    }

    df = pd.DataFrame([data])
   

    insert_into_mssql(df, 'RISKDB.dbo.katm_333_loans_overview')
