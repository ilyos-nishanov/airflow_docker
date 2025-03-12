from pymongo import MongoClient
import pyodbc

def get_mongo_client():
    return MongoClient(
        'mongodb://172.17.39.13:27017',
        username='ilyosjon.nishanov',
        password='QDXJJC6S',
        authSource='admin'
    )

def get_sql_server_connection():
    driver = 'ODBC Driver 17 for SQL Server'
    server = '172.17.17.22,54312'
    database = 'RISKDB'
    username = 'risk_technology_dev'
    password = 'tTcnjl6T'

    return pyodbc.connect(
        f"Driver={{{driver}}};"
        f"Server={server};"
        f"Database={database};"
        f"UID={username};"
        f"PWD={password};"
    )
