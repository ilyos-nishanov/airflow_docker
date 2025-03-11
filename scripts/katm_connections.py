from pymongo import MongoClient
import pyodbc

def get_mongo_client():
    return MongoClient(
        'mongodb://172.17.39.13:27017',
        username='Sardor.Maksudov',
        password='jDS3pqTV',
        authSource='admin'
    )

def get_sql_server_connection():
    driver = 'ODBC Driver 17 for SQL Server'
    server = '172.17.17.22,54312'
    database = 'RISKDB'
    username = 'SMaksudov'
    password = 'CfhljhVfrc#'

    return pyodbc.connect(
        f"Driver={{{driver}}};"
        f"Server={server};"
        f"Database={database};"
        f"UID={username};"
        f"PWD={password};"
    )
