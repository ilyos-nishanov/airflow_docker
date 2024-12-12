import os
import time
import pyodbc
import oracledb
import pandas as pd
import multiprocessing
from datetime import datetime, timedelta


def get_date_range_by_offset(offset):
    today = datetime.today().replace(day=1)  # Start from the 1st of the current month
    start_date = (today - timedelta(days=30 * offset)).replace(day=1)
    next_month = (start_date + timedelta(days=31)).replace(day=1)
    end_date = (next_month - timedelta(days=1))
    return start_date.strftime('%d-%m-%Y'), end_date.strftime('%d-%m-%Y')



def upsert_to_sql_batch(df, product):
    driver = 'ODBC Driver 17 for SQL Server'
    server = '172.17.17.22,54312'
    database = 'RISKDB'
    username = 'SMaksudov'
    password = 'CfhljhVfrc#'

    connection_mssql = pyodbc.connect(
        f"Driver={{{driver}}};Server={server};Database={database};UID={username};PWD={password};"
    )
    cursor_mssql = connection_mssql.cursor()
    df['DATE_MODIFIED'] = datetime.now().strftime('%Y-%m-%d')

    # Split DataFrame into chunks of 10,000 rows
    batch_size = 10000
    for start in range(0, len(df), batch_size):
        batch = df.iloc[start:start + batch_size]

        # Create a temporary table
        temp_table = "#TempTable"
        sql_table = 'RETAIL.FSTPD'
        primary_key_column = 'GLOB_ID'

        create_temp_table_query = f"""
            CREATE TABLE {temp_table} (
                {', '.join([f'[{col}] NVARCHAR(100)' for col in df.columns])}
            );
        """
        cursor_mssql.execute(create_temp_table_query)

        # Insert the batch into the temporary table
        insert_temp_query = f"""
            INSERT INTO {temp_table} ({', '.join(df.columns)})
            VALUES ({', '.join(['?' for _ in df.columns])})
        """
        cursor_mssql.executemany(insert_temp_query, batch.values.tolist())
        connection_mssql.commit()

        # Perform the upsert using a MERGE statement
        merge_query = f"""
            MERGE INTO {sql_table} AS Target
            USING {temp_table} AS Source
            ON Target.{primary_key_column} = Source.{primary_key_column}
            WHEN MATCHED THEN
                UPDATE SET 
                    {', '.join([f'Target.[{col}] = Source.[{col}]' for col in df.columns if col != primary_key_column])}
            WHEN NOT MATCHED BY TARGET THEN
                INSERT ({', '.join(df.columns)}) 
                VALUES ({', '.join([f'Source.[{col}]' for col in df.columns])});
        """
        cursor_mssql.execute(merge_query)
        connection_mssql.commit()

        # Drop the temporary table
        cursor_mssql.execute(f"DROP TABLE {temp_table}")

    cursor_mssql.close()
    connection_mssql.close()


def process_partition(product, month_offset):
    # Get date range for the product and month
    # start_date, end_date = get_date_range_by_offset(month_offset)
    start_date, end_date = '01-01-2022', '31-07-2024'

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
    query = f"""
            
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

    # Upsert to SQL Server in batches
    upsert_to_sql_batch(df, product)

    # Close Oracle connection
    cursor.close()
    connection.close()


# List of products and months to partition by
products = [24, 34, 32]

if __name__ == "__main__":
    start_time = datetime.now()
    print(f'Start time: {start_time}')

    # Use multiprocessing with 100 processes
    with multiprocessing.Pool(processes=100) as pool:
        pool.starmap(process_partition, [(product, month) for product in products for month in range(2, 5)])

    end_time = datetime.now()
    print(f'End time: {end_time}')
    print(f'Script took {end_time - start_time} minutes')
