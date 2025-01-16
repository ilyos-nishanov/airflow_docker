from my_connections import get_mssql_connection

sql_connection = get_mssql_connection()
sql_cursor = sql_connection.cursor()
sql_cursor.execute('truncate table bronze.[121002]')
sql_connection.commit()
sql_connection.close()