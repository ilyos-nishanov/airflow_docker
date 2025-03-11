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
mfos = [row[0] for row in cursor.fetchall()]
cursor.close()
connection.close()

# Function to load data for each mfo and month
def load_data_for_mfo(mfo, report_date_end, report_dt):
    # Oracle connection for each mfo
    connection = orc.connect(user=user, password=pwd, dsn=dsn)
    cursor = connection.cursor()
    
    query = f"""


WITH pref_cte AS (
    SELECT 
        l.id_mfo,
        l.glob_id,
        l.acc_new,
        l.kod_vals,
        l.k_man, 
        l.client,
        l.summa / (100 * 1) AS summa_val, 
        (
            SELECT 
                ABS(s.saldo / (100 * 1)) 
            FROM 
                asbt.saldo s
            WHERE  
                s.id_mfo = l.id_mfo 
                AND s.account = l.acc_new 
                AND s.dati <= TO_DATE('{report_date_end}', 'YYYY-MM-DD') 
                AND s.datn > TO_DATE('{report_date_end}', 'YYYY-MM-DD')
        ) AS ost_kr_ekv,
        l.proc_ss, 
        l.date_vyd_d, 
        l.date_pog_d,   
        NVL(
            (
                SELECT 
                    ABS(s.saldo / (100 * 1)) 
                FROM 
                    asbt.saldo s
                WHERE  
                    s.id_mfo = l.id_mfo 
                    AND s.account = (
                        SELECT 
                            ll.account 
                        FROM 
                            asbt.sp_dog_loans_acc ll 
                        WHERE 
                            ll.glob_id = l.glob_id 
                            AND ll.kod_acc = 2 
                            AND ll.rec_activ = 0 
                            AND ROWNUM = 1
                    )
                    AND s.dati <= TO_DATE('{report_date_end}', 'YYYY-MM-DD') 
                    AND s.datn > TO_DATE('{report_date_end}', 'YYYY-MM-DD')
            ),
            0
        ) AS ost_pros_ekv,
                    
        NVL(
            (SELECT ABS(s.saldo / (100 * 1)) FROM asbt.saldo s
             WHERE s.id_mfo = l.id_mfo 
                   AND s.account = (SELECT ll.account 
                                    FROM asbt.sp_dog_loans_acc ll 
                                    WHERE ll.glob_id = l.glob_id 
                                          AND ll.kod_acc = 4 
                                          AND ll.rec_activ = 0 
                                          AND ROWNUM = 1)
                   AND s.dati <= TO_DATE('{report_date_end}', 'YYYY-MM-DD') 
                   AND s.datn > TO_DATE('{report_date_end}', 'YYYY-MM-DD')), 0) AS ost_157_ekv,

        NVL(
            (SELECT ABS(s.saldo / (100 * 1)) FROM asbt.saldo s
             WHERE s.id_mfo = l.id_mfo 
                   AND s.account = (SELECT ll.account 
                                    FROM asbt.sp_dog_loans_acc ll 
                                    WHERE ll.glob_id = l.glob_id 
                                          AND ll.kod_acc = 12 
                                          AND ll.rec_activ = 0 
                                          AND ROWNUM = 1)
                   AND s.dati <= TO_DATE('{report_date_end}', 'YYYY-MM-DD') 
                   AND s.datn > TO_DATE('{report_date_end}', 'YYYY-MM-DD')), 0) AS ost_proc_ekv,

        NVL(
            (SELECT ABS(s.saldo / (100 * 1)) FROM asbt.saldo s
             WHERE s.id_mfo = l.id_mfo 
                   AND s.account = (SELECT ll.account 
                                    FROM asbt.sp_dog_loans_acc ll 
                                    WHERE ll.glob_id = l.glob_id 
                                          AND ll.kod_acc = 27 
                                          AND ll.rec_activ = 0 
                                          AND ROWNUM = 1)
                   AND s.dati <= TO_DATE('{report_date_end}', 'YYYY-MM-DD') 
                   AND s.datn > TO_DATE('{report_date_end}', 'YYYY-MM-DD')), 0) AS ost_95413_ekv,

        NVL(
            (SELECT ABS(s.saldo / (100 * 1)) FROM asbt.saldo s
             WHERE s.id_mfo = l.id_mfo 
                   AND s.account = (SELECT ll.account 
                                    FROM asbt.sp_dog_loans_acc ll 
                                    WHERE ll.glob_id = l.glob_id 
                                          AND ll.kod_acc = 21 
                                          AND ll.rec_activ = 0 
                                          AND ROWNUM = 1)
                   AND s.dati <= TO_DATE('{report_date_end}', 'YYYY-MM-DD') 
                   AND s.datn > TO_DATE('{report_date_end}', 'YYYY-MM-DD')), 0) AS ost_91501_ekv, 
                  
        NVL(
            (SELECT ABS(s.saldo / (100 * 1)) FROM asbt.saldo s
             WHERE s.id_mfo = l.id_mfo 
                   AND s.account = (SELECT ll.account 
                                    FROM asbt.sp_dog_loans_acc ll 
                                    WHERE ll.glob_id = l.glob_id 
                                          AND ll.kod_acc = 11 
                                          AND ll.rec_activ = 0 
                                          AND ROWNUM = 1)
                   AND s.dati <= TO_DATE('{report_date_end}', 'YYYY-MM-DD') 
                   AND s.datn > TO_DATE('{report_date_end}', 'YYYY-MM-DD')), 0) AS ost_16309_ekv,

        NVL(
            (SELECT ABS(s.saldo / (100 * 1)) FROM asbt.saldo s
             WHERE s.id_mfo = l.id_mfo 
                   AND s.account = (SELECT ll.account 
                                    FROM asbt.sp_dog_loans_acc ll 
                                    WHERE ll.glob_id = l.glob_id 
                                          AND ll.kod_acc = 18 
                                          AND ll.rec_activ = 0 
                                          AND ROWNUM = 1)
                   AND s.dati <= TO_DATE('{report_date_end}', 'YYYY-MM-DD') 
                   AND s.datn > TO_DATE('{report_date_end}', 'YYYY-MM-DD')), 0) AS ost_16405_ekv,

        NVL(
            (SELECT ABS(s.saldo / (100 * 1)) FROM asbt.saldo s
             WHERE s.id_mfo = l.id_mfo 
                   AND s.account = (SELECT ll.account 
                                    FROM asbt.sp_dog_loans_acc ll 
                                    WHERE ll.glob_id = l.glob_id 
                                          AND ll.kod_acc = 45 
                                          AND ll.rec_activ = 0 
                                          AND ROWNUM = 1)
                   AND s.dati <= TO_DATE('{report_date_end}', 'YYYY-MM-DD') 
                   AND s.datn > TO_DATE('{report_date_end}', 'YYYY-MM-DD')), 0) AS ost_16325_ekv,

        NVL(
            (SELECT ABS(s.saldo / (100 * 1)) FROM asbt.saldo_VAL s
             WHERE s.id_mfo = l.id_mfo 
                   AND s.account = (SELECT ll.account 
                                    FROM asbt.sp_dog_loans_acc ll 
                                    WHERE ll.glob_id = l.glob_id 
                                          AND ll.kod_acc = 4 
                                          AND ll.rec_activ = 0 
                                          AND ROWNUM = 1)
                   AND s.dati <= TO_DATE('{report_date_end}', 'YYYY-MM-DD') 
                   AND s.datn > TO_DATE('{report_date_end}', 'YYYY-MM-DD') 
                   AND l.KOD_VALS <> 0), 0) AS ost_157_NOM,

        NVL(
            (SELECT ABS(s.saldo / (100 * 1)) FROM asbt.saldo_VAL s
             WHERE s.id_mfo = l.id_mfo 
                   AND s.account = (SELECT ll.account 
                                    FROM asbt.sp_dog_loans_acc ll 
                                          WHERE ll.glob_id = l.glob_id 
                                                AND ll.kod_acc = 27 
                                                AND ll.rec_activ = 0 
                                                AND ROWNUM = 1)
                   AND s.dati <= TO_DATE('{report_date_end}', 'YYYY-MM-DD') 
                   AND s.datn > TO_DATE('{report_date_end}', 'YYYY-MM-DD') 
                   AND l.KOD_VALS <> 0), 0) AS ost_95413_NOM,

        NVL(
            (SELECT ABS(s.saldo / (100 * 1)) FROM asbt.saldo_VAL s
             WHERE s.id_mfo = l.id_mfo 
                   AND s.account = (SELECT ll.account 
                                    FROM asbt.sp_dog_loans_acc ll 
                                          WHERE ll.glob_id = l.glob_id 
                                                AND ll.kod_acc = 21 
                                                AND ll.rec_activ = 0 
                                                AND ROWNUM = 1)
                   AND s.dati <= TO_DATE('{report_date_end}', 'YYYY-MM-DD') 
                   AND s.datn > TO_DATE('{report_date_end}', 'YYYY-MM-DD')), 0) AS ost_91501_NOM,        

        l.klasskach,
        (
            SELECT 
                a.name 
            FROM 
                asbt.sp_class_activ a 
            WHERE 
                a.kod(+) = l.klasskach
        ) AS klass,

        l.k_cel_ssudy,
        (
            SELECT  
                a.name  
            FROM 
                asbt.SP_CEL_KRED a
            WHERE 
                TO_NUMBER(LPAD(a.kod_gr, 2, '0') || LPAD(a.kod_pgr, 2, '0') || LPAD(a.kod, 2, '0')) = l.k_cel_ssudy
        ) AS cel,

        l.k_vid_cred,
        (
            SELECT  
                a.name 
            FROM 
                asbt.sp_credit a 
            WHERE 
                a.kod(+) = l.k_vid_cred
        ) AS vid_kr,

        (
            SELECT 
                DECODE(ss.crit_cl, 1, 'кр/кл.', '2', 'м/бизнес', '3', 'ф/хоз', '4', 'физ.', '5', 'частн.', '6', 'Work Out кл.', '-')
            FROM 
                asbt.sp_dog_loans_status ss 
            WHERE 
                ss.glob_id = l.glob_id
        ) AS criter

    FROM  
        asbt.sp_dog_loans l
    WHERE l.id_mfo = {mfo}
        AND l.mejbank IN (0, 1, 3, 4) 
        AND l.kod_sost NOT IN (9, 10) 
        AND rec_activ = 0 
        AND (
            l.ss_status <> 3   
            OR (l.ss_status = 3 AND NVL(l.date_stop, l.date_start) > TO_DATE('{report_date_end}', 'YYYY-MM-DD'))
        )   
    ORDER BY 
        l.kod_vals, 
        l.glob_id
)
,

cte AS (
    SELECT 
        pc.glob_id, 
        pc.acc_new, 
        pc.id_mfo, 
        pc.kod_vals,
        pc.k_man, 
        pc.client,
        pc.summa_val, 
        pc.ost_pros_ekv, 
        pc.ost_157_ekv, 
        pc.ost_proc_ekv,
        pc.ost_kr_ekv,
        pc.proc_ss,
        pc.date_vyd_d,
        pc.date_pog_d,
        pc.ost_95413_ekv,
        pc.ost_91501_ekv,
        pc.ost_16309_ekv,
        pc.OST_16405_EKV,
        pc.OST_157_NOM,
        pc.OST_95413_NOM,
        pc.OST_91501_NOM,
        pc.KLASSKACH,
        pc.KLASS,
        pc.K_CEL_SSUDY,
        pc.cel,
        pc.VID_KR,
        pc.K_VID_CRED,
        pc.CRITER,
        CASE 
            WHEN pc.ost_pros_ekv <> 0 
                THEN asbt.DEF_DAT_SAL_VALp(pc.id_mfo, sdla.account, SUBSTR(sdla.account, 6, 3), TO_DATE('{report_date_end} 00:00:00', 'YYYY-MM-DD HH24:MI:SS'))
            ELSE NULL
        END AS ost_pros_ekv_dpd,

        CASE 
            WHEN pc.ost_157_ekv <> 0 
                THEN asbt.DEF_DAT_SAL_VALp(pc.id_mfo, sdla_2.account, SUBSTR(sdla_2.account, 6, 3), TO_DATE('{report_date_end} 00:00:00', 'YYYY-MM-DD HH24:MI:SS'))
            ELSE NULL
        END AS ost_157_ekv_dpd,

        CASE 
            WHEN pc.ost_proc_ekv <> 0 
                THEN asbt.DEF_DAT_SAL_VALp(pc.id_mfo, sdla_3.account, SUBSTR(sdla_3.account, 6, 3), TO_DATE('{report_date_end} 00:00:00', 'YYYY-MM-DD HH24:MI:SS'))
            ELSE NULL
        END AS ost_proc_ekv_dpd,

        '{report_dt}' AS report_dt
        
        
          
    FROM pref_cte pc
    LEFT JOIN asbt.SP_DOG_LOANS_ACC sdla
    ON pc.glob_id = sdla.glob_id AND sdla.KOD_ACC = 2 AND sdla.REC_ACTIV = 0

    LEFT JOIN asbt.SP_DOG_LOANS_ACC sdla_2
    ON pc.glob_id = sdla_2.glob_id AND sdla_2.KOD_ACC = 4 AND sdla_2.REC_ACTIV = 0

    LEFT JOIN asbt.SP_DOG_LOANS_ACC sdla_3
    ON pc.glob_id = sdla_3.glob_id AND sdla_3.KOD_ACC = 12 AND sdla_3.REC_ACTIV = 0

    WHERE NVL(pc.ost_pros_ekv, 0) + NVL(pc.ost_proc_ekv, 0) + NVL(pc.ost_157_ekv, 0) +
          NVL(pc.ost_95413_ekv, 0) + NVL(pc.ost_91501_ekv, 0) <> 0
)

SELECT *
FROM cte
    """
    cursor.execute(query)
    result = cursor.fetchall()

    # Convert the result to a pandas DataFrame
    df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
    df.columns = [col if col else 'Unnamed_column' for col in df.columns]

    # Close the Oracle connection
    cursor.close()
    connection.close()

    # SQL Server connection for each mfo
    driver = 'SQL Server'
    server = '172.17.17.22,54312'
    database = 'RISKDB'
    username = 'SMaksudov'  # Replace with your username
    password = 'CfhljhVfrc#'  # Replace with your password

    connection_mssql = odbc.connect(
        "Driver={" + driver + "};"
        "Server=" + server + ";"
        "Database=" + database + ";"
        "UID=" + username + ";"
     "PWD=" + password + ";"
        )

    # driver = 'SQL Server'
    # server = '192.168.80.28,65064'
    # database = 'RISKDB'
    table_name = 'ilyos_test_tbl'
    
    # connection_mssql = odbc.connect("Driver={" + driver + "};"
    #                                 "Server=" + server + ";"
    #                                 "Database=" + database + ";"
    #                                 "Trusted_Connection=yes;")
    cursor_mssql = connection_mssql.cursor()

    # Check if the table exists, and if not, create it
    check_table_query = f"""
    IF OBJECT_ID(N'[{table_name}]', 'U') IS NULL
    BEGIN
        CREATE TABLE [{table_name}] ( {', '.join([f'[{col}] NVARCHAR(500)' for col in df.columns])} )
    END;
    """
    cursor_mssql.execute(check_table_query)
    
    for index, row in df.iterrows():
        # Modify the INSERT query to handle special characters and Unicode
        insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in range(len(df.columns))])})"

        # Convert values to strings to handle special characters
        values = [str(val) for val in row]

        cursor_mssql.execute(insert_query, tuple(values))

    connection_mssql.commit()

    # Close the SQL Server connection
    cursor_mssql.close()
    connection_mssql.close()

# Loop through each mfo and load data for each month
# for month in range(6, 7):
#     # Calculate start and end dates for the month
#     # report_date_start = datetime(2023, month, 1).strftime('%Y-%m-%d')
#     # report_date_end = (datetime(2023, month, 1) + timedelta(days=32)).replace(day=1) - timedelta(days=1)
#     # report_date_end = report_date_end.strftime('%Y-%m-%d')
#     # report_dt = datetime(2023, month, 1).strftime('%Y%m')
report_date_end = '2024-08-30'
report_dt = '202408'
iteration_count = 0
print(datetime.now())
for mfo in mfos:
    load_data_for_mfo(mfo, report_date_end, report_dt)
    iteration_count += 1
    print(f"Iteration {iteration_count}: Processed mfo {mfo} for {report_dt}")
print(datetime.now())