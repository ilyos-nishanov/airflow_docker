from _121002_connections import get_mssql_connection

sql_connection = get_mssql_connection()
sql_cursor = sql_connection.cursor()
query = f"""insert into bronze.[121002_backup] 
select *, cast(getdate() as date) as DATE_MODIFIED from bronze.[121002]"""
sql_cursor.execute(query)
sql_connection.commit()
sql_connection.close()