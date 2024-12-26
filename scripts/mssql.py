
from connections import get_mongo_client, get_sql_server_connection


def max_number_find():
    conn = get_sql_server_connection()
    cursor = conn.cursor()

    # Check the maximum number in the specified table
    check_table_query = f"""SELECT MAX(number) FROM RISKDB.dbo.contracts_task123_katm077;"""
    cursor.execute(check_table_query)

    # Fetch the result (the single number)
    result = cursor.fetchone()

    # Extract the maximum number or return None if no results exist
    max_number = int(result[0] if result else None)
    return max_number


max_num = max_number_find()

# Function to insert DataFrame into MSSQL in chunks
def insert_into_mssql(df, table_name):
    conn = get_sql_server_connection()
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