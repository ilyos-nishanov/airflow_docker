import pyodbc
from time import time, sleep
from datetime import datetime, timedelta
from my_connections import get_mssql_connection


###################################### DECORATOR FUNCTION #########################################################

def retry_with_relogin(retries=3, delay=5):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(1, retries + 1):
                try:
                    return func(*args, **kwargs)
                except pyodbc.OperationalError as e:
                    if "Login timeout expired" in str(e):
                        print(f"Login timeout error occurred on attempt {attempt}: {e}")
                        if attempt < retries:
                            print("Attempting to re-establish connection...")
                            print(f"Retrying in {delay} seconds...")
                            sleep(delay)
                        else:
                            print("All retry attempts failed.")
                            raise
                    else:
                        raise
        return wrapper
    return decorator

#########################################################################################################################################################

def get_date_range():

    today = datetime.today()
    first_day_of_current_month = today.replace(day=1)
    end_date = first_day_of_current_month.strftime('%d-%m-%Y')
    start_date = (first_day_of_current_month - timedelta(days=90)).replace(day=1).strftime('%d-%m-%Y')
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
@retry_with_relogin(retries=5, delay=10)
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

def upsert(query):
    connection = get_mssql_connection()
    cursor = connection.cursor()
    cursor.execute(query)
    connection.commit()
    cursor.close()
    connection.close()

#########################################################################################################################################################

def generate_date_ranges(start_year, end_year, period="month"):

    if period not in {"day", "week", "month", "season"}:
        raise ValueError("Invalid period. Supported values are: 'day', 'week', 'month', 'season'.")

    date_ranges = []
    current_date = datetime(start_year, 1, 1)
    today = datetime.now()
    end_date = min(datetime(end_year, 12, 31), today)

    while current_date <= end_date:
        if period == "day":
            start_date = current_date
            next_date = start_date + timedelta(days=1)
            end_of_range = start_date
        elif period == "week":
            start_date = current_date
            next_date = start_date + timedelta(weeks=1)
            end_of_range = min(next_date - timedelta(days=1), end_date)
        elif period == "month":
            start_date = current_date
            next_month = current_date.replace(day=28) + timedelta(days=4)
            end_of_range = min(next_month.replace(day=1) - timedelta(days=1), end_date)
            next_date = end_of_range + timedelta(days=1)
        elif period == "season":
            start_date = current_date
            # Seasons: Winter (Dec-Feb), Spring (Mar-May), Summer (Jun-Aug), Fall (Sep-Nov)
            if start_date.month in {12, 1, 2}:  # Winter
                season_end_month = 2
            elif start_date.month in {3, 4, 5}:  # Spring
                season_end_month = 5
            elif start_date.month in {6, 7, 8}:  # Summer
                season_end_month = 8
            else:  # Fall
                season_end_month = 11

            # Calculate the end date of the season
            season_end_year = start_date.year
            if start_date.month == 12:
                season_end_year += 1

            end_of_range = datetime(season_end_year, season_end_month, 1) + timedelta(days=31)
            end_of_range = end_of_range.replace(day=1) - timedelta(days=1)
            end_of_range = min(end_of_range, end_date)
            next_date = end_of_range + timedelta(days=1)

        date_ranges.append((start_date.strftime('%Y-%m-%d'), end_of_range.strftime('%Y-%m-%d')))
        current_date = next_date

    return date_ranges
