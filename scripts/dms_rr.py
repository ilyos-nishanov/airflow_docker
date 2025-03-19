import pandas as pd
from dms_connections import get_oracle_connection, get_mssql_connection

def fetch_and_write_data():

    connection = None
    sql_connection = None

    try:
        connection = get_oracle_connection()
        cursor = connection.cursor()
        query = f"""

        select * from FTEST_DMS_MZ_SERVICE.DMM_VARIABLES
        
        """
        cursor.execute(query)
        result = cursor.fetchall()

        df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
    
        # sql_connection = get_mssql_connection()
        # sql_cursor = sql_connection.cursor()
        
        # for _, row in df.iterrows():
        #     insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in range(len(df.columns))])})"
        #     sql_cursor.execute(insert_query, tuple(row))
        # sql_connection.commit()
        
    finally:
        if connection is not None:
            connection.close()
        if sql_connection is not None:
            sql_connection.close()

    # return f"Finished writing data for range {date_range}"
    return df

df = fetch_and_write_data()
print(df.head())