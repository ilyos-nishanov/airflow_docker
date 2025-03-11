#%%
import psycopg2
from psycopg2 import sql
import pandas as pd
from pymongo import MongoClient

#%% Define MongoDB, OracleDB, and SQL Server connections
MONGO_URI = "mongodb://192.168.0.103:27017/?directConnection=true&serverSelectionTimeoutMS=5000&appName=mongosh+2.3.3"
#172.24.0.179
#192.168.0.103

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
def load_to_postgres(data):
    conn = psycopg2.connect("dbname=postgres user=airflow password=airflow host=192.168.0.103 port=5432")
    cursor = conn.cursor()

    # Create table if not exists with correct column name (_id)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS public.my_dw_table (
            _id SERIAL PRIMARY KEY,
            name VARCHAR(255),
            borough VARCHAR(255)
        )
    """)

    # Insert or Update data
    for _, row in data.iterrows():
        cursor.execute("""
            INSERT INTO my_dw_table (_id, name, borough)
            VALUES (%s, %s, %s)
            ON CONFLICT (_id) 
            DO UPDATE SET name = EXCLUDED.name, borough = EXCLUDED.borough
        """, (row['_id'], row['name'], row['borough']))

    # Commit the transaction
    conn.commit()
    conn.close()


# Extract data and print the number of records extracted
data = extract_from_mongodb()
print(f"Data extracted: {len(data)} records")

# Transform data if needed
data = transform_data(data)
print("Data transformation complete.")

# Load data into PostgreSQL
print("Starting data load to PostgreSQL...")
load_to_postgres(data)
print("Data load to PostgreSQL complete.")

# %%
