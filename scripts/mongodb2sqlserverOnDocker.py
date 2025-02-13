#%%
import psycopg2
import pandas as pd
from pymongo import MongoClient
import pyodbc

#%% Define MongoDB and SQL Server connections
MONGO_URI = "mongodb://192.168.0.103:27017/?directConnection=true&serverSelectionTimeoutMS=5000&appName=mongosh+2.3.3"
#172.24.0.179
#192.168.0.103

# SQL Server connection string for a Docker container (example)
SQL_SERVER_CONN_STR = "Driver={ODBC Driver 18 for SQL Server};Server=192.168.0.103,1433;Database=forAirflow;UID=sa;PWD=Valuetech@123;TrustServerCertificate=yes;"

#%%
def extract_from_mongodb():
    client = MongoClient(MONGO_URI)
    db = client.admin  # db name
    data = db.restaurants.find()  # Fetch data
    df = pd.DataFrame(list(data))  # Convert cursor to list, then to DataFrame
    return df

#%%
def transform_data(data):
    # Transformation logic here
    polished_data = data  # Placeholder for transformed data
    return polished_data

#%%
def load_to_sql_server(data):
    # Connect to SQL Server
    conn = pyodbc.connect(SQL_SERVER_CONN_STR)
    cursor = conn.cursor()

    # Create table if not exists with correct column names (_id)
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='my_dw_table' AND xtype='U')
        CREATE TABLE my_dw_table (
            _id INT IDENTITY(1,1) PRIMARY KEY,
            name NVARCHAR(255),
            borough NVARCHAR(255)
        )
    """)

    # Insert or Update data (you can use a MERGE statement or check for duplicates)
    for _, row in data.iterrows():
        cursor.execute("""
            IF EXISTS (SELECT 1 FROM my_dw_table WHERE _id = ?)
            BEGIN
                UPDATE my_dw_table SET name = ?, borough = ? WHERE _id = ?
            END
            ELSE
            BEGIN
                INSERT INTO my_dw_table (name, borough) VALUES (?, ?)
            END
        """, (row['_id'], row['name'], row['borough'], row['_id'], row['name'], row['borough']))

    # Commit the transaction and close the connection
    conn.commit()
    conn.close()

# Extract data and print the number of records extracted
data = extract_from_mongodb()
print(f"Data extracted: {len(data)} records")

# Transform data if needed
data = transform_data(data)
print("Data transformation complete.")

# Load data into SQL Server
print("Starting data load to SQL Server...")
load_to_sql_server(data)
print("Data load to SQL Server complete.")
