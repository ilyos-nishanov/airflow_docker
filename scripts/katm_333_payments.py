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
    check_table_query = f"""select max(number) from RISKDB.dbo.katm_333_payments;
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
            payments = loan.get('payments')
            
            if not payments or payments == '' or (isinstance(payments, (list, dict)) and not payments):
                # Skip if overdue_principals is empty, missing, or an empty string
                continue
            if isinstance(payments, dict):
                # If payments is a single dictionary
                payment = payments.get('payment')
                if isinstance(payment, dict):
                    # Single dictionary case for payment
                    row = {
                        'number': number,
                        'uuid': uuid,
                        'client_id': client_id,
                        'payment_date': payment.get('payment_date', ''),
                        'payment_amount': payment.get('payment_amount', ''),
                        'currency': payment.get('currency', ''),
                        'payment_amount_principal': payment.get('payment_amount_principal', ''),
                        'payment_amount_interest': payment.get('payment_amount_interest', ''),
                        'payment_amount_other': payment.get('payment_amount_other', ''),
                        'total_payment_amount': payment.get('total_payment_amount', ''),
                        'total_payment_principal': payment.get('total_payment_principal', ''),
                        'total_payment_interest': payment.get('total_payment_interest', ''),
                        'total_payment_other': payment.get('total_payment_other', ''),
                        'payment_terms_compliance': payment.get('payment_terms_compliance')

                    }
                    data.append(row)
                elif isinstance(payment, list):
                    # List of dictionaries case for payment
                    for item in payment:
                        my_item= item.get('payment')
                        if isinstance(item, dict):
                            row = {
                            'number': number,
                            'uuid': uuid,
                            'client_id': client_id,
                            'payment_date': item.get('payment_date', ''),
                            'payment_amount': item.get('payment_amount', ''),
                            'currency': item.get('currency', ''),
                            'payment_amount_principal': item.get('payment_amount_principal', ''),
                            'payment_amount_interest': item.get('payment_amount_interest', ''),
                            'payment_amount_other': item.get('payment_amount_other', ''),
                            'total_payment_amount': item.get('total_payment_amount', ''),
                            'total_payment_principal': item.get('total_payment_principal', ''),
                            'total_payment_interest': item.get('total_payment_interest', ''),
                            'total_payment_other': item.get('total_payment_other', ''),
                            'payment_terms_compliance': item.get('payment_terms_compliance')
                            
                        }
                            data.append(row)
            
            elif isinstance(payments, list):
                # If payments is a list of dictionaries
                for payments_item in payments:
                    payment = payments_item.get('payment')
                    if isinstance(payment, dict):
                        # Single dictionary case for payment within the list
                        row = {
                        'number': number,
                        'uuid': uuid,
                        'client_id': client_id,
                        'payment_date': payments_item.get('payment_date', ''),
                        'payment_amount': payments_item.get('payment_amount', ''),
                        'currency': payments_item.get('currency', ''),
                        'payment_amount_principal': payments_item.get('payment_amount_principal', ''),
                        'payment_amount_interest': payments_item.get('payment_amount_interest', ''),
                        'payment_amount_other': payments_item.get('payment_amount_other', ''),
                        'total_payment_amount': payments_item.get('total_payment_amount', ''),
                        'total_payment_principal': payments_item.get('total_payment_principal', ''),
                        'total_payment_interest': payments_item.get('total_payment_interest', ''),
                        'total_payment_other': payments_item.get('total_payment_other', ''),
                        'payment_terms_compliance': payments_item.get('payment_terms_compliance')
                    }
                        data.append(row)
                    elif isinstance(payment, list):
                        # List of dictionaries case for payment within the list
                        for item in payment:
                            row = {
                                'number': number,
                                'uuid': uuid,
                                'client_id': client_id,
                                'payment_date': item.get('payment_date', ''),
                                'payment_amount': item.get('payment_amount', ''),
                                'currency': item.get('currency', ''),
                                'payment_amount_principal': item.get('payment_amount_principal', ''),
                                'payment_amount_interest': item.get('payment_amount_interest', ''),
                                'payment_amount_other': item.get('payment_amount_other', ''),
                                'total_payment_amount': item.get('total_payment_amount', ''),
                                'total_payment_principal': item.get('total_payment_principal', ''),
                                'total_payment_interest': item.get('total_payment_interest', ''),
                                'total_payment_other': item.get('total_payment_other', ''),
                                'payment_terms_compliance': item.get('payment_terms_compliance')
                            }
                            data.append(row)
        # rows.append(row)
            
    if data:
        df = pd.DataFrame(data)
        insert_into_mssql(df, 'RISKDB.dbo.katm_333_payments')
            