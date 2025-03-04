import pyodbc
import logging
import traceback
import pandas as pd
from tqdm import tqdm
from functools import wraps
from time import time, sleep
from bson.errors import InvalidBSON
from datetime import datetime, timedelta
from pymongo.errors import CursorNotFound, PyMongoError
from katm_connections import get_sql_server_connection, get_mongo_client


###################################### DECORATOR FUNCTION #########################################################

def retry_process_entry(retries=3, delay=5):  # for mongodb
    """Decorator to retry the process_entry function for recoverable errors."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            entry_name = args[0].get("name", "Unknown Entry")  # Extract the entry name for logging
            for attempt in range(1, retries + 1):
                try:
                    return func(*args, **kwargs)  # Call the process_entry function
                except Exception as e:
                    error_message = f"Error processing {entry_name} on attempt {attempt}: {e}"
                    log_error(error_message)  # Log the error
                    print(error_message)
                    traceback.print_exc()  # Print the full stack trace for debugging
                    if attempt < retries:
                        print(f"Retrying {entry_name} in {delay} seconds...")
                        sleep(delay)  # Wait before retrying
                    else:
                        print(f"All retry attempts failed for {entry_name}.")
                        raise
        return wrapper
    return decorator

###################################### DECORATOR FUNCTION #########################################################

def retry_with_relogin(retries=3, delay=5):  # for insert_into_mssql
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

###################################################################################################################

def log_error(message):
    with open('error_log.txt', "a") as error_log:
        error_log.write(f"{message}\n")

###################################################################################################################

def convert_date(date_str):
    if date_str:
        try:
            # First, check if the date is in the YYYYMMDD format (no separators)
            if len(date_str) == 8 and date_str.isdigit():
                # Convert YYYYMMDD to DD.MM.YYYY
                date_str = f"{date_str[6:8]}.{date_str[4:6]}.{date_str[:4]}"
            
            # Try parsing known formats
            for fmt in ['%d.%m.%Y', '%d.%m.%y', '%d/%m/%Y', '%d-%m-%Y', '%Y-%m-%d', '%Y.%m.%d']:
                try:
                    date_obj = datetime.strptime(date_str, fmt)
                    return date_obj.strftime('%Y-%m-%d')  # Return in YYYY-MM-DD format
                except ValueError:
                    continue  # Try the next format
            
            # If no format matched, raise an error
            raise ValueError(f"Unsupported date format: {date_str}")
        except Exception as e:
            print(f"Error converting date: {date_str} - {e}")
    return None  # Return None if the date is invalid or empty

####################################################################################################################

def get_date_range_by_offset(offset):
    today = datetime.today().replace(day=1)  # Start from the 1st of the current month
    start_date = (today - timedelta(days=30 * offset)).replace(day=1)
    next_month = (start_date + timedelta(days=31)).replace(day=1)
    end_date = (next_month - timedelta(days=1))
    return start_date.strftime('%d-%m-%Y'), end_date.strftime('%d-%m-%Y')

####################################################################################################################

def load_columns(file_path):
    with open(file_path, 'r') as file:
        columns = file.read().splitlines()
    return columns

####################################################################################################################

def map_dff_to_columns(dff, columns):
    final_df = pd.DataFrame(columns)
    
    # Fill in the columns of `final_df` with values from `dff`
    for col in final_df.columns:
        if col in dff.columns:
            final_df[col] = dff[col]  # Copy the values from `dff`
        else:
            # final_df[col] = None  # Set the column to None if not in `dff`
            final_df[col] = None 
    
    return final_df

####################################################################################################################

def max_number_find_with_schema(table_name, schema ='bronze'):
    conn = get_sql_server_connection()
    cursor = conn.cursor()

    # Check if table exists, if not create it
    check_table_query = f"""select max(number) from {schema}.{table_name};
    """
    cursor.execute(check_table_query)

    # Fetch the result (the single number)
    result = cursor.fetchone()

    # result will be a tuple with one element, so you can extract the number like this:
    max_number = int(result[0] if result else None)
    return max_number

####################################################################################################################

def max_number_find(table_name):
    conn = get_sql_server_connection()
    cursor = conn.cursor()

    # Check if table exists, if not create it
    check_table_query = f"""select max(number) from {table_name};
    """
    cursor.execute(check_table_query)

    # Fetch the result (the single number)
    result = cursor.fetchone()

    # result will be a tuple with one element, so you can extract the number like this:
    max_number = int(result[0] if result else None)
    return max_number

####################################################################################################################

def max_number_find_with_cast(table_name):
    conn = get_sql_server_connection()
    cursor = conn.cursor()

    # Check if table exists, if not create it
    check_table_query = f"""select max(cast(number as int)) from {table_name};
    """
    cursor.execute(check_table_query)

    # Fetch the result (the single number)
    result = cursor.fetchone()

    # result will be a tuple with one element, so you can extract the number like this:
    max_number = int(result[0] if result else None)
    return max_number
####################################################################################################################

@retry_with_relogin(retries=5, delay=10)
def insert_into_mssql_tqdm(df, table_name):
    conn = get_sql_server_connection()
    cursor = conn.cursor()


    check_table_query = f"""
    IF OBJECT_ID(N'{table_name}', 'U') IS NULL
    BEGIN
        CREATE TABLE {table_name}  (
    {', '.join([f'[{col}] NVARCHAR(1000)' for col in df.columns])}
);
    END;"""
    cursor.execute(check_table_query)
    conn.commit()


    for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Inserting rows", unit="row"):
        try:
            insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in range(len(df.columns))])})"
            values = [str(val) for val in row]
            cursor.execute(insert_query, tuple(values))
        except Exception as e:
            print(f"Error inserting row {index}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

####################################################################################################################

@retry_with_relogin(retries=5, delay=10)
def insert_into_mssql(df, table_name):
    conn = get_sql_server_connection()
    cursor = conn.cursor()


    check_table_query = f"""
    IF OBJECT_ID(N'{table_name}', 'U') IS NULL
    BEGIN
        CREATE TABLE {table_name}  (
    {', '.join([f'[{col}] NVARCHAR(1000)' for col in df.columns])}
);
    END;"""
    cursor.execute(check_table_query)
    conn.commit()


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

####################################################################################################################

def number_is_present(table_name, num):
    conn = get_sql_server_connection()
    cursor = conn.cursor()

    # Check if table exists, if not create it
    check_table_query = f"""select 1 from {table_name} where number = {num};
    """
    cursor.execute(check_table_query)

####################################################################################################################

def clean_nested_columns(df):
    # Remove columns with lists or dictionaries
    for col in df.columns:
        if any(isinstance(val, (list, dict)) for val in df[col]):
            df = df.drop(columns=[col])
    return df

####################################################################################################################

def get_contract_id(table_name):
    conn = get_sql_server_connection()
    cursor = conn.cursor()

    # Check if table exists
    check_table_exists_query = f"""
    IF OBJECT_ID('{table_name}', 'U') IS NOT NULL
        SELECT 1
    ELSE
        SELECT 0
    """
    cursor.execute(check_table_exists_query)
    table_exists = cursor.fetchone()[0]

    if not table_exists:
        print(f"Table '{table_name}' does not exist.")
        return []

    # Fetch numbers from the table
    try:
        get_contract_id_query = f"SELECT contract_id FROM {table_name};"
        cursor.execute(get_contract_id_query)
        result = cursor.fetchall()
        return [row[0] for row in result]
    except Exception as e:
        print(f"Error fetching contract_id from table '{table_name}': {e}")
        return []
    finally:
        cursor.close()
        conn.close()

####################################################################################################################

def clean_and_map_columns(df, file_path, essential_columns=None):

    # Remove columns with lists or dictionaries
    for col in df.columns:
        if any(isinstance(val, (list, dict)) for val in df[col]):
            df = df.drop(columns=[col])

    # Load column names from the file
    with open(file_path, 'r') as file:
        columns = file.read().splitlines()

    # Ensure essential columns are included
    essential_columns = essential_columns or []
    for col in essential_columns:
        if col not in columns:
            columns.append(col)

    # Create a new DataFrame with the specified columns
    result_df = pd.DataFrame(columns=columns)

    # Map values from the original DataFrame, defaulting to None for missing columns
    for col in columns:
        result_df[col] = df[col] if col in df.columns else None

    return result_df

####################################################################################################################

def clean_and_map_columns_with_parsing(df, file_path, essential_columns=None):
    """
    Cleans a DataFrame by flattening nested dicts and exploding lists, then maps its values
    to a new DataFrame with specified columns loaded from a text file.

    Args:
        df (pd.DataFrame): The source DataFrame to process.
        file_path (str): Path to the text file containing column names (one per line).
        essential_columns (list, optional): List of essential columns to ensure in the output.

    Returns:
        pd.DataFrame: A new DataFrame with the specified columns, flattened and mapped.
    """
    def flatten_column(col):
        """
        Flattens a column containing dicts or lists.
        - Dicts are expanded into separate columns.
        - Lists are converted to strings (or can be exploded into multiple rows if needed).
        """
        if col.apply(lambda x: isinstance(x, dict)).any():
            # If dict, expand into multiple columns
            expanded_df = pd.json_normalize(col.dropna())
            return expanded_df
        elif col.apply(lambda x: isinstance(x, list)).any():
            # If list, convert to strings (or use `.explode()` for multiple rows)
            return col.apply(lambda x: ', '.join(map(str, x)) if isinstance(x, list) else x)
        return col
    
    # Flatten nested columns
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (list, dict))).any():
            flattened = flatten_column(df[col])
            if isinstance(flattened, pd.DataFrame):
                # Add expanded columns back to the DataFrame
                df = pd.concat([df.drop(columns=[col]), flattened.add_prefix(f"{col}_")], axis=1)
            else:
                df[col] = flattened

    # Load column names from the file
    with open(file_path, 'r') as file:
        columns = file.read().splitlines()

    # Ensure essential columns are included
    essential_columns = essential_columns or []
    for col in essential_columns:
        if col not in columns:
            columns.append(col)

    # Create a new DataFrame with the specified columns
    result_df = pd.DataFrame(columns=columns)

    # Map values from the original DataFrame, defaulting to None for missing columns
    for col in columns:
        result_df[col] = df[col] if col in df.columns else None

    return result_df

####################################################################################################################

# Function to create an empty DataFrame with specified columns
def create_empty_df_with_columns(columns):
    return pd.DataFrame(columns=columns)

# Function to load columns from a text file
def load_my_columns(file_path):
    with open(file_path, 'r') as file:
        columns = file.read().splitlines()  # Read each line as a column name
    return columns

# Function to map `dff` values to a DataFrame with specified columns
def map_dff_to_my_columns(dff, my_columns):
    # Ensure '_id' and 'number' are in the my_columns list
    essential_columns = ['_id', 'number']
    for col in essential_columns:
        if col not in my_columns:
            my_columns.append(col)

    # Create an empty DataFrame with the columns
    final_df = create_empty_df_with_columns(my_columns)

    # Fill in the columns of `final_df` with values from `dff`
    for col in final_df.columns:
        if col in dff.columns:
            final_df[col] = dff[col]  # Copy the values from `dff`
        else:
            final_df[col] = None  # Set the column to None if not in `dff`

    return final_df

####################################################################################################################

def get_numbers(table_name, foreign_key, top=1000000):
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    query = f"""
        
                select top {top} o.number
                from bronze.katm_077_overview o
                left join {table_name} c
                on o.number = c.number
                where c.number is null
                and o.{foreign_key} != '0'

    """
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return [row[0] for row in result]
    except Exception as e:
        print(f"Error fetching contract_id from table '{table_name}': {e}")
        return []
    finally:
        cursor.close()
        conn.close()
####################################################################################################################

def get_numbers_2(query):
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return [row[0] for row in result]
    except Exception as e:
        print(e)
        return []
    finally:
        cursor.close()
        
####################################################################################################################

def map_dff_to_my_columns_2(dff, my_columns):
    final_df = create_empty_df_with_columns(my_columns)
    for col in final_df.columns:
        if col in dff.columns:
            final_df[col] = dff[col]  
        else:
            final_df[col] = None
    return final_df

####################################################################################################################

def resolve_nested_field(data, path):
    keys = path.split('.')
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key, {})
        else:
            return None  # Path does not exist
    return data
    
####################################################################################################################

def setup_table(write_to_table, columns, schema = 'bronze'):
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    column_definitions = ', '.join([f"[{col}] NVARCHAR(MAX)" for col in columns])
    query = f"""
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{write_to_table}')
    BEGIN
        CREATE TABLE {schema}.{write_to_table} (
            {column_definitions}
        )
    END
    """
    try:
        cursor.execute(query)
        conn.commit()
    except Exception as e:
        print(f"Error creating table: {e}")
    finally:
        cursor.close()
        conn.close()

####################################################################################################################

@retry_with_relogin(retries=5, delay=10)
def insert_into_mssql_with_schema(df, table_name, schema = 'bronze'):
    table_with_schema = f"{schema}.[{table_name}]"   # will only insert without creating
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    for index, row in df.iterrows():
        try:
            insert_query = f"INSERT INTO {table_with_schema} VALUES ({', '.join(['?' for _ in range(len(df.columns))])})"
            values = [str(val) for val in row]
            cursor.execute(insert_query, tuple(values))
        except Exception as e:
            print(f"Error inserting row {index}: {e}")

    conn.commit()
    cursor.close()
    conn.close()

####################################################################################################################

@retry_with_relogin(retries=5, delay=10) 
def insert_batch_into_mssql_with_schema_2(df, table_name, schema='bronze'):
    table_with_schema = f"{schema}.[{table_name}]"   
    i = None
    conn = get_sql_server_connection()  
    cursor = conn.cursor()
    
    for index, row in df.iterrows():
        try:
            i +=1
            insert_query = f"INSERT INTO {table_with_schema} VALUES ({', '.join(['?' for _ in range(len(df.columns))])})"    
            values = [str(val) for val in row]
            cursor.execute(insert_query, tuple(values))
            if i%1000==0:
                conn.commit()
                cursor.close()
                conn.close()
                conn = get_sql_server_connection()  # Reconnect if needed
                cursor = conn.cursor()
        except (pyodbc.OperationalError, pyodbc.InterfaceError, pyodbc.Error) as e:
            print(f"Error inserting row {index}: {e}")
            if "Communication link failure" in str(e):
                print("Reconnecting to SQL Server...")
                sleep(5)  # Small delay before retry
    conn.commit()
    cursor.close()
    conn.close()

####################################################################################################################

def flatten_dict(d, parent_key='', sep='.'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

####################################################################################################################

logging.basicConfig(
    filename='error_log.txt',  # Log file name
    level=logging.ERROR,       # Log only errors or higher levels
    format='%(asctime)s - %(levelname)s - %(message)s'  # Log format
)

@retry_process_entry(retries=3, delay=5)
def process_document_batch_with_logging(doc, field_path, columns, write_to_table):
    try:
        rows = []
        _id = str(doc.get('_id'))
        number = doc.get('number')
        fields = resolve_nested_field(doc, field_path)

        if isinstance(fields, dict):
            fields = [fields]
        elif fields is None:
            fields = []

        for field in fields:
            row = {'_id': _id, 'number': number, **field}
            rows.append(row)

        if rows:
            # Create a DataFrame from the rows and map it to the required columns
            df = pd.DataFrame(rows)
            final_df = map_dff_to_my_columns(df, columns)
            insert_into_mssql_with_schema(final_df, write_to_table)

    except InvalidBSON as e:
        # Log the error and skip the problematic document
        logging.error(f"Skipping document with _id: {_id} due to InvalidBSON error: {e}")
        return
    except Exception as e:
        # Log any unexpected errors and skip the problematic document
        logging.error(f"An error occurred while processing document with _id: {_id}. Error: {e}")
        return

####################################################################################################################

@retry_process_entry(retries=3, delay=5)
def parse_doc(entry, docs_cursor, field_path, columns, write_to_table):
    try:
        start_time = time()
        for doc in docs_cursor:
            rows = []
            _id = str(doc.get('_id'))
            number = doc.get('number')
            fields = resolve_nested_field(doc, field_path)
            if isinstance(fields, dict):
                fields = [fields]
            for field in fields:
                row = {'_id': _id, 'number': number, **field}
                rows.append(row)
            if rows:
                df = pd.DataFrame(rows)
                final_df = map_dff_to_my_columns(df, columns)
                insert_into_mssql_with_schema(final_df, write_to_table)

        elapsed_time = time() - start_time
        print(f"Completed processing for: {entry['name']} in {elapsed_time:.2f} seconds")
    except Exception as e:
        print(f"Error processing {entry['name']}: {e}")

####################################################################################################################

def process_document(doc, field_path, columns, write_to_table):
    try:
        rows = []
        _id = str(doc.get('_id'))
        number = doc.get('number')
        fields = resolve_nested_field(doc, field_path)

        if isinstance(fields, dict):
            fields = [fields]
        elif fields is None:
            fields = []

        for field in fields:
            row = {'_id': _id, 'number': number, **field}
            rows.append(row)

        if rows:
            df = pd.DataFrame(rows)
            final_df = map_dff_to_my_columns(df, columns)
            insert_into_mssql_with_schema(final_df, write_to_table)
    except Exception as e:
        logging.error(f"Error processing document with _id: {_id}. Error: {e}")

####################################################################################################################

def fetch_all_documents_with_retry(task_collection, query, projection, retries=3, delay=5):
    attempt = 0
    while attempt < retries:
        try:
            client = get_mongo_client()
            task_collection = client['task']['task']  # Reinitialize the task collection
            cursor = task_collection.find(query, projection, no_cursor_timeout=True)
            return cursor  # Return the cursor directly for iteration
        except CursorNotFound as e:
            attempt += 1
            logging.error(f"CursorNotFound error: {e}. Retrying {attempt}/{retries} in {delay} seconds...")
            sleep(delay)
        except PyMongoError as e:
            logging.error(f"Unexpected MongoDB error: {e}. Retrying {attempt}/{retries} in {delay} seconds...")
            attempt += 1
            sleep(delay)
    raise RuntimeError("Max retries reached. Unable to fetch documents from MongoDB.")

####################################################################################################################

def extract_fields(data, key=None, rows=None):
    if rows is None:
        rows = []

    if isinstance(data, dict):
        for k, v in data.items():
            if k == key and isinstance(v, dict): 
                row = {field: value for field, value in v.items() if not isinstance(value, (list, dict))}
                rows.append(row)
            elif k == key and isinstance(v, list): 
                for item in v:
                    if isinstance(item, dict):
                        row = {field: value for field, value in item.items() if not isinstance(value, (list, dict))}
                        rows.append(row)
            elif isinstance(v, (dict, list)):
                extract_fields(v, key, rows)

    elif isinstance(data, list):
        for item in data:
            extract_fields(item, key, rows)

    return pd.DataFrame(rows) if rows else pd.DataFrame()

####################################################################################################################

def select(query):
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return [row[0] for row in result]
    except Exception as e:
        print(e)
        return []
    finally:
        cursor.close()
        
####################################################################################################################