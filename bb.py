#%%
import pyodbc
import pandas as pd
from pymongo import MongoClient
#%% Define MongoDB, OracleDB, and SQL Server connections
MONGO_URI = "mongodb://127.0.0.1:27017/?directConnection=true&serverSelectionTimeoutMS=2000&appName=mongosh+2.3.3"
SQL_SERVER_URI="mssql+pyodbc://SA:Valuetech@123@localhost:1433/forAirflow?driver=ODBC+Driver+18+for+SQL+Server"
#%%
def extract_from_mongodb():
    client = MongoClient(MONGO_URI)
    db = client.admin #db name
    data = db.restaurants.find()  # Fetch data
    # return data
    df = pd.DataFrame(list(data))  # Convert cursor to list, then to DataFrame
    return df
#%%
def transform_data(data):
    # Transformation logic here
    polished_data = data  # Placeholder for transformed data
    return polished_data
#%%
def load_to_sql_server(data):
    conn = pyodbc.connect(SQL_SERVER_URI)
    cursor = conn.cursor()
    # Load data to SQL Server
    cursor.execute("""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='my_dw_table' AND xtype='U')
        CREATE TABLE my_dw_table (
            _id INT PRIMARY KEY,
            name NVARCHAR(255),
            borough NVARCHAR(255)
        )
    """)
    
    # Insert or Update data
    for row in data:
        cursor.execute("""
            MERGE INTO my_dw_table AS target
            USING (SELECT ? AS _id, ? AS name, ? AS borough) AS source
            ON target._id = source._id
            WHEN MATCHED THEN 
                UPDATE SET target.name = source.name, target.borough = source.borough
            WHEN NOT MATCHED THEN
                INSERT (_id, name, borough) VALUES (source._id, source.name, source.borough);
        """, row['_id'], row['name'], row['borough'])

    # Commit the transaction
    conn.commit()
    conn.close()
#%%
df=extract_from_mongodb()
print(df)
#%%
load_to_sql_server(df)

# %%
