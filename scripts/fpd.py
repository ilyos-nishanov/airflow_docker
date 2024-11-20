import os
import time
import pyodbc
import oracledb
import functools
import pandas as pd
import multiprocessing
from datetime import datetime, timedelta

def retry_on_failure(max_retries=3, delay=5, exceptions=(Exception,)):
    """Retry decorator for handling failures with exponential backoff."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    retries += 1
                    if retries < max_retries:
                        print(f"Error: {e}. Retrying {retries}/{max_retries} in {delay} seconds...")
                        time.sleep(delay)
                        delay *= 2  # Exponential backoff
                    else:
                        print(f"Failed after {max_retries} attempts: {e}")
                        raise
        return wrapper
    return decorator


def get_date_range_by_offset(offset):
    today = datetime.today().replace(day=1)  # Start from the 1st of the current month
    start_date = (today - timedelta(days=30 * offset)).replace(day=1)
    next_month = (start_date + timedelta(days=31)).replace(day=1)
    end_date = (next_month - timedelta(days=1))
    return start_date.strftime('%d-%m-%Y'), end_date.strftime('%d-%m-%Y')

# Function to save DataFrame to CSV
def save_to_csv(df, product, month):
    file_name = f"../data/fpd/output_product_{product}_month_{month}.csv"
    df.to_csv(file_name, index=False, mode='w', header=True)
    print(f"Data saved to {file_name}")

# Function to write to SQL Server with retry
@retry_on_failure(max_retries=5, delay=3, exceptions=(pyodbc.ProgrammingError, pyodbc.InterfaceError))
def write_to_sql(df, product):
    driver = 'ODBC Driver 17 for SQL Server'
    server = '172.17.17.22,54312'
    database = 'RISKDB'
    username = 'SMaksudov'  # Replace with your username
    password = 'CfhljhVfrc#'  # Replace with your password

    connection_mssql = pyodbc.connect(
        "Driver={" + driver + "};"
        "Server=" + server + ";"
        "Database=" + database + ";"
        "UID=" + username + ";"
        "PWD=" + password + ";"
    )

    cursor_mssql = connection_mssql.cursor()
    
    check_table_query = f"""
IF NOT EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES 
               WHERE TABLE_SCHEMA = 'RETAIL' AND TABLE_NAME = 'FPD{product}')
BEGIN
    CREATE TABLE RETAIL.FPD{product} (
        {', '.join([f'[{col}] NVARCHAR(500)' for col in df.columns])}
    );
END
"""
    cursor_mssql.execute(check_table_query)
    
    for _, row in df.iterrows():
        insert_query = f"INSERT INTO RETAIL.FPD{product} VALUES ({', '.join(['?' for _ in range(len(df.columns))])})"
        values = [str(val) for val in row]
        cursor_mssql.execute(insert_query, tuple(values))

    connection_mssql.commit()
    cursor_mssql.close()
    connection_mssql.close()

# Main processing function with retry
@retry_on_failure(max_retries=5, delay=3, exceptions=(oracledb.DatabaseError, oracledb.OperationalError))
def process_partition(product, month_offset):
    # Get date range for the product and month
    start_date, end_date = get_date_range_by_offset(month_offset)

    # Oracle DB connection parameters
    connection_params = {
        "user": "sardor",
        "password": "Maksudov01Test",
        "dsn": oracledb.makedsn("192.168.81.99", "1521", service_name="orcl1")
    }

    # Connect to Oracle DB
    oracledb.init_oracle_client()
    connection = oracledb.connect(**connection_params)
    cursor = connection.cursor()

    # Query to fetch data based on the product and date range
    with open('../queries/fpd.sql', 'r') as file:
        query = file.read()

    cursor.execute("alter session set nls_date_format = 'DD-MM-YYYY'")
    cursor.execute(query)
    result = cursor.fetchall()

    # Convert the result to a pandas DataFrame
    df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])

    # Save to CSV
    save_to_csv(df, product, month_offset)

    # Write to SQL Server
    write_to_sql(df, product)

    # Close Oracle connection
    cursor.close()
    connection.close()

# List of products and months to partition by (3 products x 3 months = 9 partitions)
products = [24, 34, 32]

# Create a pool of processes
if __name__ == "__main__":
    start_time=datetime.now()
    print(f'start time: {start_time}')

    with multiprocessing.Pool(processes=100) as pool:
        # Assign each process a combination of product and month
        pool.starmap(process_partition, [(product, month) for product in products for month in range(2, 5)])
    
    end_time=datetime.now()
    print(f'end time: {end_time}')
    print(f'script took {end_time-start_time} minutes')
