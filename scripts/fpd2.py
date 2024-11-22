import os
import time
import pyodbc
import oracledb
import functools
import pandas as pd
import multiprocessing
from datetime import datetime, timedelta

def retry_on_failure(max_retries=3, delay=5, exceptions=(Exception,)):
    """Retry decorator for handling failures with exponential backoff."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            current_delay = delay
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    retries += 1
                    if retries < max_retries:
                        print(f"Error: {e}. Retrying {retries}/{max_retries} in {current_delay} seconds...")
                        time.sleep(current_delay)
                        current_delay *= 2  # Exponential backoff
                    else:
                        print(f"Failed after {max_retries} attempts: {e}")
                        raise
        return wrapper
    return decorator


def get_date_range_by_offset(offset):
    today = datetime.today().replace(day=1)  # Start from the 1st of the current month
    start_date = (today - timedelta(days=30 * offset)).replace(day=1)
    next_month = (start_date + timedelta(days=31)).replace(day=1)
    end_date = (next_month - timedelta(days=1))
    return start_date.strftime('%d-%m-%Y'), end_date.strftime('%d-%m-%Y')

# Function to save DataFrame to CSV
def save_to_csv(df, product, month):
    file_name = f"data/fpd/output_product_{product}_month_{month}.csv"
    df.to_csv(file_name, index=False, mode='w', header=True)
    print(f"Data saved to {file_name}")


def initialize_sql_table(driver, server, database, username, password, table_name, columns):
    """Initializes the SQL table by dropping it if it exists and creating a new one."""
    connection_mssql = pyodbc.connect(
        "Driver={" + driver + "};"
        "Server=" + server + ";"
        "Database=" + database + ";"
        "UID=" + username + ";"
        "PWD=" + password + ";"
    )

    cursor_mssql = connection_mssql.cursor()
    drop_table_query = f"""
        IF OBJECT_ID('{table_name}', 'U') IS NOT NULL
        DROP TABLE {table_name};
    """
    cursor_mssql.execute(drop_table_query)
    
    create_table_query = f"""
        CREATE TABLE {table_name} ({', '.join([f'[{col}] NVARCHAR(500)' for col in columns])})
    """
    cursor_mssql.execute(create_table_query)
    
    connection_mssql.commit()
    cursor_mssql.close()
    connection_mssql.close()


# Function to write to SQL Server with retry
@retry_on_failure(max_retries=5, delay=3, exceptions=(pyodbc.ProgrammingError, pyodbc.InterfaceError))
def write_to_sql(df, product):
    driver = 'ODBC Driver 17 for SQL Server'
    server = '172.17.17.22,54312'
    database = 'RISKDB'
    username = 'SMaksudov'  
    password = 'CfhljhVfrc#' 

    connection_mssql = pyodbc.connect(
        "Driver={" + driver + "};"
        "Server=" + server + ";"
        "Database=" + database + ";"
        "UID=" + username + ";"
        "PWD=" + password + ";"
    )

    cursor_mssql = connection_mssql.cursor()
        
    for _, row in df.iterrows():
        insert_query = f"INSERT INTO RETAIL.FPD VALUES ({', '.join(['?' for _ in range(len(df.columns))])})"
        values = [str(val) for val in row]
        cursor_mssql.execute(insert_query, tuple(values))

    connection_mssql.commit()
    cursor_mssql.close()
    connection_mssql.close()

# Main processing function with retry
@retry_on_failure(max_retries=5, delay=3, exceptions=(oracledb.DatabaseError, oracledb.OperationalError))
def process_partition(product, month_offset):
    # Get date range for the product and month
    start_date, end_date = get_date_range_by_offset(month_offset)

    # Oracle DB connection parameters
    connection_params = {
        "user": "sardor",
        "password": "Maksudov01Test",
        "dsn": oracledb.makedsn("192.168.81.99", "1521", service_name="orcl1")
    }

    # Connect to Oracle DB
    oracledb.init_oracle_client()
    connection = oracledb.connect(**connection_params)
    cursor = connection.cursor()

    # Query to fetch data based on the product and date range
    query = \
            \
    f"""
  WITH  DEL_SALDO AS 
    ( SELECT 
    t.GLOB_ID,t.DATE_VYD_D,S1.DATE_DELQ,o.ACCOUNT,o.KOD_ACC,t.K_VID_CRED,
    MAX(S1.DATN) OVER (PARTITION BY T.GLOB_ID ORDER BY T.GLOB_ID) AS DATN,
    s1.PAYM_CUMULAT,S1.DELQ_CUMULAT
    FROM asbt.sp_dog_loans t 
    INNER JOIN asbt.sp_dog_loans_acc o
    on t.glob_id=o.glob_id
    and o.kod_acc IN(2,12) 
    LEFT JOIN  ( SELECT ACCOUNT,dati AS DATE_DELQ,(SUM(OBOR_C)over (partition by account order by dati))/100 as PAYM_CUMULAT,                     
                        dense_rank() over (partition by account,ID_MFO order by obor_d_y) as RANK,DATN,ID_MFO,
                        OBOR_D_Y/100 AS DELQ_CUMULAT
                FROM ASBT.SALDO WHERE OBOR_D_Y<>0)s1
        on o.account=s1.ACCOUNT  
        WHERE 1=1  
        AND t.DATE_VYD_D between '{start_date}' and '{end_date}'
        AND t.REC_ACTIV=0
        AND t.K_VID_CRED in ({product})      
        AND t.ID_MFO=S1.ID_MFO ),
        
    DEL_DATE_GASH AS (
    SELECT 
    t.GLOB_ID,gs.DATE_GASH AS FIRST_PAYM_DATE, gs1.DATE_GASH as SECOND_PAYM_DATE,gs2.DATE_GASH as THIRD_PAYM_DATE
    FROM asbt.sp_dog_loans t 
    left join  ( SELECT GLOB_ID,DATE_GASH,DENSE_RANK() over (partition by glob_id order by DATE_GASH) AS RANK FROM asbt.sp_dog_loans_ggs ) gs
        on t.glob_id=gs.glob_id 
    left join  ( SELECT GLOB_ID,DATE_GASH,DENSE_RANK() over (partition by glob_id order by DATE_GASH) AS RANK FROM asbt.sp_dog_loans_ggs ) gs1
        on t.glob_id=gs1.glob_id 
    left join  ( SELECT GLOB_ID,DATE_GASH,DENSE_RANK() over (partition by glob_id order by DATE_GASH) AS RANK FROM asbt.sp_dog_loans_ggs ) gs2
        on t.glob_id=gs2.glob_id     
    WHERE 1=1
    AND t.K_VID_CRED in ({product}) 
    AND t.DATE_VYD_D between '{start_date}' and '{end_date}'
    AND t.REC_ACTIV=0
    AND GS.RANK=1
    AND GS1.RANK=2
    AND GS2.RANK=3
    ), 

    RANK_A AS 
    (SELECT 
        t.GLOB_ID,O.ACCOUNT,S1.DATI,S1.FIRST_DELQ,
        MIN(S1.DATI) OVER (PARTITION BY S1.ACCOUNT,S1.RANK order by s1.DATI) AS MIN_DATE_A             
    FROM asbt.sp_dog_loans t 
    LEFT JOIN asbt.sp_dog_loans_acc o
        on t.glob_id=o.glob_id
        and o.kod_acc IN(2,12) 
    LEFT JOIN  ( SELECT ACCOUNT,DATI,
                dense_rank() over (partition by account order by obor_d_y) as RANK,ID_MFO,
                OBOR_D_Y/100 AS FIRST_DELQ
                FROM ASBT.SALDO WHERE OBOR_D_Y<>0)s1
        on o.account=s1.ACCOUNT
    WHERE 1=1  
    AND t.K_VID_CRED in ({product}) 
    AND t.DATE_VYD_D between '{start_date}' and '{end_date}'  
    AND t.REC_ACTIV=0
    AND S1.RANK=1
    AND t.ID_MFO=S1.ID_MFO
    ),
    
    RANK_B AS
    (SELECT 
        t.GLOB_ID,O.ACCOUNT,S1.DATI,S1.SECOND_DELQ,
        MIN(S1.DATI) OVER (PARTITION BY S1.ACCOUNT,S1.RANK order by s1.DATI) AS MIN_DATE_B                
    FROM asbt.sp_dog_loans t 
    LEFT JOIN asbt.sp_dog_loans_acc o
        on t.glob_id=o.glob_id
        and o.kod_acc IN(2,12) 
    left join  ( SELECT ACCOUNT,DATI, dense_rank() over (partition by account order by obor_d_y) as RANK,
                    ID_MFO,OBOR_D_Y/100 AS second_DELQ
                FROM ASBT.SALDO WHERE OBOR_D_Y<>0)s1
    on o.account=s1.ACCOUNT
    WHERE 1=1  
    AND t.DATE_VYD_D between '{start_date}' and '{end_date}'
    AND t.K_VID_CRED  in ({product})   
    AND t.REC_ACTIV=0    
    AND S1.RANK=2 --SECOND DELIQUIENCY     
    ),
    RANK_C AS
    (SELECT 
        t.GLOB_ID,O.ACCOUNT,S1.DATI,S1.THIRD_DELQ,
        MIN(S1.DATI) OVER (PARTITION BY S1.ACCOUNT,S1.RANK order by s1.DATI) AS MIN_DATE_C         
    FROM asbt.sp_dog_loans t 
    LEFT JOIN asbt.sp_dog_loans_acc o
        on t.glob_id=o.glob_id
        and o.kod_acc IN(2,12)
    left join  ( SELECT ACCOUNT,DATI,
                dense_rank() over (partition by account order by obor_d_y) as RANK,ID_MFO,
                OBOR_D_Y/100 AS THIRD_DELQ
                FROM ASBT.SALDO WHERE OBOR_D_Y<>0)s1
    on o.account=s1.ACCOUNT
    WHERE 1=1  
        AND t.K_VID_CRED in ({product})
        AND t.DATE_VYD_D between '{start_date}' and '{end_date}'
        AND t.REC_ACTIV=0
        AND S1.RANK=3 --THIRD DELIQUIENCY
        ),
    
    PRE_ITOG AS
    ( SELECT
        d.GLOB_ID,d.DATE_VYD_D,d.ACCOUNT,d.KOD_ACC,d.DATE_DELQ,D.DATN,d.K_VID_CRED,
        d.PAYM_CUMULAT,d.DELQ_CUMULAT,dg.FIRST_PAYM_DATE,dg.SECOND_PAYM_DATE,dg.THIRD_PAYM_DATE,
        RA.FIRST_DELQ,RA.MIN_DATE_A,RB.SECOND_DELQ,RB.MIN_DATE_B,RC.THIRD_DELQ,RC.MIN_DATE_C,     
        CASE WHEN(ra.MIN_DATE_A-dg.FIRST_PAYM_DATE)<=5
            AND (d.PAYM_CUMULAT>=rA.FIRST_DELQ)  
            THEN (d.DATE_DELQ-ra.MIN_DATE_A) end as test,   
        MIN (CASE WHEN(ra.MIN_DATE_A-dg.FIRST_PAYM_DATE)<=5
            AND (d.PAYM_CUMULAT>=rA.FIRST_DELQ)
            THEN (d.DATE_DELQ-ra.MIN_DATE_A)                                         
        WHEN (d.PAYM_CUMULAT=0 OR d.PAYM_CUMULAT<ra.FIRST_DELQ)
            AND  RA.FIRST_DELQ>0
            AND (ra.MIN_DATE_A-FIRST_PAYM_DATE)<=5
            THEN round((DATN- ra.MIN_DATE_A))
            ELSE 10000 end )  OVER (PARTITION BY D.ACCOUNT ORDER BY D.ACCOUNT) AS FPD,         
        MIN(CASE WHEN  d.PAYM_CUMULAT<rA.FIRST_DELQ
                AND  RA.FIRST_DELQ>0
                AND (rb.MIN_DATE_B-dg.SECOND_PAYM_DATE)<=5        
                THEN (DATN- rb.MIN_DATE_b)                                    
                ELSE 0 END) OVER (PARTITION BY D.ACCOUNT ORDER BY D.ACCOUNT) AS SPD_1,                                       
        MIN(CASE WHEN (d.PAYM_CUMULAT=0 or d.PAYM_CUMULAT<rA.FIRST_DELQ)
                AND  RA.FIRST_DELQ>0
                AND (rA.MIN_DATE_A-dg.FIRST_PAYM_DATE)>=5
                THEN round((DATN- ra.MIN_DATE_A))          
            ELSE 0 END) OVER (PARTITION BY D.ACCOUNT ORDER BY D.ACCOUNT) AS SPD_2,                
        MIN(CASE WHEN (d.PAYM_CUMULAT=0 OR d.PAYM_CUMULAT<rA.FIRST_DELQ) 
                AND  RA.FIRST_DELQ>0
                AND (rc.MIN_DATE_C-dg.THIRD_PAYM_DATE)<=5
                THEN round((DATN- rc.MIN_DATE_C))           
                ELSE 0 END) OVER (PARTITION BY D.ACCOUNT ORDER BY D.ACCOUNT) AS TPD_1,              
        MIN(CASE WHEN (d.PAYM_CUMULAT=0 OR d.PAYM_CUMULAT<rA.FIRST_DELQ)
                AND  RA.FIRST_DELQ>0
                AND (rB.MIN_DATE_B-dg.SECOND_PAYM_DATE)>=5
                THEN round((DATN- rB.MIN_DATE_B))       
                ELSE 0 END) OVER (PARTITION BY D.ACCOUNT ORDER BY D.ACCOUNT) AS TPD_2 
    FROM del_saldo d
    INNER join del_date_gash  dg
    on d.glob_id=dg.glob_id
    INNER JOIN RANK_A  ra
    on d.account=ra.account
    LEFT JOIN RANK_B  rb
    on d.account=rb.account  
    LEFT JOIN RANK_C  rc
    on d.account=rc.account  
    WHERE (MIN_DATE_A-SECOND_PAYM_DATE)<21)
    
    SELECT DISTINCT 
    DATE_VYD_D, GLOB_ID, K_VID_CRED,
    MAX( CASE WHEN FPD=10000 THEN 0 ELSE FPD END) OVER (PARTITION BY GLOB_ID ORDER BY GLOB_ID) AS FPD,
    MAX(CASE WHEN SPD_1=0 THEN SPD_2 ELSE SPD_1 END) OVER (PARTITION BY GLOB_ID ORDER BY GLOB_ID) AS SPD ,     
    MAX(CASE WHEN TPD_1=0 THEN TPD_2 ELSE TPD_1 END) OVER (PARTITION BY GLOB_ID ORDER BY GLOB_ID) AS TPD 
    FROM pre_itog
    where fpd NOT IN (0,10000)
    
    """
    cursor.execute("alter session set nls_date_format = 'DD-MM-YYYY'")
    cursor.execute(query)
    result = cursor.fetchall()

    # Convert the result to a pandas DataFrame
    df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])

    # # Save to CSV
    # save_to_csv(df, product, month_offset)

    # Write to SQL Server
    write_to_sql(df, product)

    # Close Oracle connection
    cursor.close()
    connection.close()


# Create a pool of processes
if __name__ == "__main__":
    start_time=datetime.now()
    print(f'start time: {start_time}')

    # SQL connection details
    driver = 'ODBC Driver 17 for SQL Server'
    server = '172.17.17.22,54312'
    database = 'RISKDB'
    username = 'SMaksudov'
    password = 'CfhljhVfrc#'
    table_name = 'RETAIL.FPD'

    # Initialize SQL table once before multiprocessing
    sample_columns = ['DATE_VYD_D', 'GLOB_ID', 'K_VID_CRED', 'FPD', 'SPD', 'TPD']
    initialize_sql_table(driver, server, database, username, password, table_name, sample_columns)

    # List of products and months to partition by (3 products x 3 months = 9 partitions)
    products = [24, 34, 32]

    with multiprocessing.Pool(processes=100) as pool:
        # Assign each process a combination of product and month
        pool.starmap(process_partition, [(product, month) for product in products for month in range(2, 5)])
    
    end_time=datetime.now()
    print(f'end time: {end_time}')
    print(f'script took {end_time-start_time} minutes')