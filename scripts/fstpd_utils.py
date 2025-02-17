import time
from datetime import datetime, timedelta
from fstpd_connections import get_mssql_connection

#########################################################################################################################################################

def get_date_range():

    today = datetime.today()
    first_day_of_current_month = today.replace(day=1)
    end_date = first_day_of_current_month.strftime('%d-%m-%Y')
    start_date = (first_day_of_current_month - timedelta(days=120)).replace(day=1).strftime('%d-%m-%Y')
    return start_date, end_date

#########################################################################################################################################################

def initialize_sql_table(table_name, columns):

    connection = get_mssql_connection()
    cursor = connection.cursor()
    
    drop_table_query = f"""
        IF OBJECT_ID('{table_name}', 'U') IS NOT NULL
        DROP TABLE {table_name};
    """
    create_table_query = f"""
        CREATE TABLE {table_name} ({', '.join([f'[{col}] NVARCHAR(500)' for col in columns])})
    """
    
    cursor.execute(drop_table_query)
    cursor.execute(create_table_query)
    connection.commit()
    cursor.close()
    connection.close()

#########################################################################################################################################################

def write_to_sql(df, table_name):
    connection = get_mssql_connection()
    cursor = connection.cursor()
    
    for _, row in df.iterrows():
        insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in range(len(df.columns))])})"
        values = [str(val) for val in row]
        cursor.execute(insert_query, tuple(values))
    
    connection.commit()
    cursor.close()
    connection.close()

#########################################################################################################################################################

def retry_on_failure(max_retries=3, delay=5, exceptions=(Exception,)):
    """Retry decorator for handling failures with exponential backoff."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            current_delay = delay
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    retries += 1
                    if retries < max_retries:
                        print(f"Error: {e}. Retrying {retries}/{max_retries} in {current_delay} seconds...")
                        time.sleep(current_delay)
                        current_delay *= 2  # Exponential backoff
                    else:
                        print(f"Failed after {max_retries} attempts: {e}")
                        raise
        return wrapper
    return decorator

#########################################################################################################################################################

def upsert(query):
    connection = get_mssql_connection()
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    cursor.close()
    connection.close()

#########################################################################################################################################################