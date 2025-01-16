import oracledb
import pyodbc


SQL_CREDENTIALS = {
    "driver": "ODBC Driver 17 for SQL Server",
    "server": "172.17.17.22,54312",
    "database": "RISKDB",
    "username": "risk_technology_dev",
    "password": "tTcnjl6T",
}


ORACLE_CREDENTIALS = {
        "user": "NishanovII",
        "password": "hfSAjVOqH0Q5",
        "dsn": oracledb.makedsn("192.168.81.115", "1521", service_name="orcl1")
}

def get_mssql_connection():
    creds = SQL_CREDENTIALS
    return pyodbc.connect(
        f"Driver={{{creds['driver']}}};"
        f"Server={creds['server']};"
        f"Database={creds['database']};"
        f"UID={creds['username']};"
        f"PWD={creds['password']};"
    )

def get_oracle_connection():
    creds = ORACLE_CREDENTIALS
    oracledb.init_oracle_client()
    return oracledb.connect(user=creds["user"], password=creds["password"], dsn=creds["dsn"])
