from _121002_connections import get_mssql_connection

sql_connection = get_mssql_connection()
sql_cursor = sql_connection.cursor()
sql_cursor.execute('truncate table bronze.bulk_insert_test')
sql_connection.commit()
sql_connection.close()