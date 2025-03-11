import os
import pyodbc as odbc
import pandas as pd
import oracledb as orc
from datetime import datetime, timedelta

# Initialize Oracle client
orc.init_oracle_client()

# Oracle connection parameters
dsn = "192.168.81.99:1521/orcl1"
user = "sardor"  # Replace with your username
pwd = "Maksudov01Test"  # Replace with your password

# Fetch unique id_mfo values
connection = orc.connect(user=user, password=pwd, dsn=dsn)
cursor = connection.cursor()
cursor.execute("SELECT DISTINCT id_mfo FROM asbt.sp_dog_loans")
mfos = [row[0] for row in cursor.fetchall()]  # Fetch and store all unique id_mfo values
cursor.close()  # Close cursor after fetching data
connection.close()  # Close Oracle connection

print("Connection successful")

# SQL Server connection parameters
server = '172.17.17.22,54312'
database = 'RISKDB'
username = 'SMaksudov'  # Replace with your username
password = 'CfhljhVfrc#'  # Replace with your password

# Connect to SQL Server
connection_mssql = odbc.connect(
    f"Driver={{ODBC Driver 17 for SQL Server}};"
    f"Server={server};"
    f"Database={database};"
    f"UID={username};"
    f"PWD={password};"
)

table_name = 'myMFO'  # Table where you'll insert data
cursor_mssql = connection_mssql.cursor()

# Check if the table exists, and if not, create it
create_table_query = f"""
select @@version
"""
cursor_mssql.execute(create_table_query)
myVersion=cursor_mssql.fetchall()
print(myVersion)
# # Insert mfo values into SQL Server as integer column
# insert_query = f"""
# INSERT INTO [{table_name}] (id_mfo)
# VALUES (?)
# """
# for mfo in mfos:
#     cursor_mssql.execute(insert_query, (mfo,))  # Insert each mfo value as a tuple
# connection_mssql.commit()  # Commit the changes to SQL Server

# Close the SQL Server connection
cursor_mssql.close()
connection_mssql.close()

# print(f"Successfully inserted {len(mfos)} mfo values into the SQL Server table.")





# IF OBJECT_ID(N'[{table_name}]', 'U') IS NULL
# BEGIN
#     CREATE TABLE [{table_name}] (
#         id_mfo INT
#     )
# END;