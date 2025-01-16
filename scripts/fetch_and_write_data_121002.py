import pandas as pd
from my_connections import get_oracle_connection, get_mssql_connection
from my_utils import retry_with_relogin

# @retry_with_relogin(retries=5, delay=10)
def fetch_and_write_data(date_range, table_name):

    connection = None
    sql_connection = None

    try:
        connection = get_oracle_connection()
        cursor = connection.cursor()
        query = f"""
        SELECT
            NVL(l.id_mfo, 0) AS id_mfo,
            NVL(l.kod_vals, 0) AS kod_val,
            NVL(l.client, '0') AS client,
            NVL(to_char(l.kod_vals, '000') || '-' || v.name, '0') AS name_val,
            NVL(l.acc_new, '0') AS acc_new,
            NVL(p.account, '0') AS acc_pros,
            NVL(r.account, '0') AS acc_ras,
            NVL(l.proc_ss, 0) AS proc_ss,
            NVL(l.proc_pr, 0) AS proc_pr,
            NVL(l.summa / 1000, 0) AS summa,
            -- Handle out-of-range dates
            CASE
                WHEN l.date_vyd_d < TO_DATE('1753-01-01', 'YYYY-MM-DD') THEN TO_DATE('1753-01-01', 'YYYY-MM-DD')
                ELSE NVL(l.date_vyd_d, TO_DATE('1900-01-01', 'YYYY-MM-DD'))
            END AS date_vyd_d,
            CASE
                WHEN l.date_pog_d < TO_DATE('1753-01-01', 'YYYY-MM-DD') THEN TO_DATE('1753-01-01', 'YYYY-MM-DD')
                ELSE NVL(l.date_pog_d, TO_DATE('1900-01-01', 'YYYY-MM-DD'))
            END AS date_pog_d,
            CASE
                WHEN DECODE(l.ss_status, 3, l.date_stop, NULL) < TO_DATE('1753-01-01', 'YYYY-MM-DD') THEN TO_DATE('1753-01-01', 'YYYY-MM-DD')
                ELSE NVL(DECODE(l.ss_status, 3, l.date_stop, NULL), TO_DATE('1900-01-01', 'YYYY-MM-DD'))
            END AS dat_pogash,
            NVL(b.name, '0') AS name_cel,
            NVL(c.name, '0') AS name_sost,
            NVL(L.K_MAN, 0) AS K_MAN,
            NVL(L.GLOB_ID, 0) AS GLOB_ID,
            NVL(L.ID_APL, 0) AS ID_APL,
            NVL(l.kreditospos || '_' || vk.KLASS_NAME, '0') AS n_kreditospos,
            NVL(l.inspect || '_' || se.fio, '0') AS inspect,
            NVL(l.ss_status, 0) AS ss_status,
            CASE
                WHEN l.date_stop < TO_DATE('1753-01-01', 'YYYY-MM-DD') THEN TO_DATE('1753-01-01', 'YYYY-MM-DD')
                ELSE NVL(l.date_stop, TO_DATE('1900-01-01', 'YYYY-MM-DD'))
            END AS date_stop,
            NVL(DECODE(l.k_commis, 0, 'Процент от суммы кредита', 1, 'Фиксир.сумма', 2, 'Наличная сумма', 'нет'), 'нет') AS k_commis,
            NVL(DECODE(l.k_cred_line, 1, 'Откр.', 2, 'Откр.с умен.лимита', 'закрытыя'), 'закрытыя') AS n_line,
            NVL(l.val_com, 0) AS val_com,
            NVL(l.pr_serv, 0) AS pr_serv,
            NVL(l.k_vid_cred, 0) AS k_vid_cred,
            NVL(l.k_type_obesp, 0) AS k_type_obesp,
            NVL(l.sumobesp / 1000, 0) AS s_obesp,
            NVL(sa.id_client, 0) AS id_cl,
            CASE
                WHEN sc.pas_date_exp_director < TO_DATE('1753-01-01', 'YYYY-MM-DD') THEN TO_DATE('1753-01-01', 'YYYY-MM-DD')
                ELSE NVL(sc.pas_date_exp_director, TO_DATE('1900-01-01', 'YYYY-MM-DD'))
            END AS pas_end,
            NVL(sc.post_adr, '0') AS post_adr,
            CASE
                WHEN sc.date_reg < TO_DATE('1753-01-01', 'YYYY-MM-DD') THEN TO_DATE('1753-01-01', 'YYYY-MM-DD')
                ELSE NVL(sc.date_reg, TO_DATE('1900-01-01', 'YYYY-MM-DD'))
            END AS date_reg,
            NVL(sc.num_nalog, 0) AS num_nalog,
            NVL(sc.kod_nalog_org || '_' || vr.NAME, '0') AS n_gni,
            NVL(sc.pas_ser_director, '0') AS ser_pas,
            NVL(sc.pas_num_director, '0') AS num_pas,
            NVL(vs.kod || ' ' || vs.name, '0') AS n_sector,
            NVL(vi.name, '0') AS n_ist,
            NVL(vo.kod || ' ' || vo.name, '0') AS n_obl,
            NVL(vre.kod || ' ' || vre.name, '0') AS n_reg,
            CASE
                WHEN sc.date_birth < TO_DATE('1753-01-01', 'YYYY-MM-DD') THEN TO_DATE('1753-01-01', 'YYYY-MM-DD')
                ELSE NVL(sc.date_birth, TO_DATE('1900-01-01', 'YYYY-MM-DD'))
            END AS date_birth,
            CASE
                WHEN (SELECT MAX(d.date_end) FROM asbt.sp_client_vid_dejat d WHERE d.id_client = sa.id_client) < TO_DATE('1753-01-01', 'YYYY-MM-DD') THEN TO_DATE('1753-01-01', 'YYYY-MM-DD')
                ELSE NVL((SELECT MAX(d.date_end) FROM asbt.sp_client_vid_dejat d WHERE d.id_client = sa.id_client), TO_DATE('1900-01-01', 'YYYY-MM-DD'))
            END AS liz_end,
            CASE
                WHEN (SELECT MIN(ss.dati) FROM asbt.saldo ss WHERE ss.id_mfo = l.id_mfo AND ss.account = l.acc_new AND ss.dati >= l.date_vyd_d AND ss.obor_d <> 0) < TO_DATE('1753-01-01', 'YYYY-MM-DD') THEN TO_DATE('1753-01-01', 'YYYY-MM-DD')
                ELSE NVL((SELECT MIN(ss.dati) FROM asbt.saldo ss WHERE ss.id_mfo = l.id_mfo AND ss.account = l.acc_new AND ss.dati >= l.date_vyd_d AND ss.obor_d <> 0), TO_DATE('1900-01-01', 'YYYY-MM-DD'))
            END AS dat_vyd,
            NVL((SELECT name FROM asbt.sp_credit WHERE kod = l.k_vid_cred), '0') AS n_vid_cred,
            NVL((SELECT name FROM asbt.sp_obesp_kred WHERE kod = l.k_type_obesp), '0') AS n_obesp,
            NVL((SELECT ll.COUNT_WORK_PLACE FROM asbt.sp_loans_apl ll WHERE ll.glob_id = l.id_apl AND ll.rec_activ = 0), 0) AS kol_work
        FROM
            asbt.sp_dog_loans l
            JOIN asbt.sp_accounts sa ON l.id_mfo = sa.id_mfo AND l.acc_new = sa.account_new
            JOIN asbt.sp_client sc ON sa.id_client = sc.id_client
            LEFT JOIN asbt.sp_dog_loans_sost c ON c.kod = l.kod_sost
            LEFT JOIN asbt.sp_val v ON v.kod = l.kod_vals
            LEFT JOIN asbt.v_sp_cel_kred b ON b.kod = l.k_cel_ssudy
            LEFT JOIN asbt.sp_sector vs ON vs.kod = sc.kod_sector
            LEFT JOIN asbt.v_sp_istochn vi ON vi.kod = l.k_ist_cred
            LEFT JOIN asbt.sp_klass vk ON vk.KLASS_ID = l.kreditospos
            LEFT JOIN asbt.v_sp_nalog_org vr ON vr.KOD = sc.kod_nalog_org
            LEFT JOIN asbt.v_sp_region vo ON vo.KOD = sc.kod_obl
            LEFT JOIN asbt.v_sp_reg vre ON vre.KOD = sc.kod_reg
            LEFT JOIN asbt.sp_emp se ON se.kod = l.inspect AND se.id_mfo = l.id_mfo
            LEFT JOIN asbt.sp_dog_loans_acc p ON p.glob_id = l.glob_id AND p.rec_activ = 0 AND p.kod_acc = 2
            LEFT JOIN asbt.sp_dog_loans_acc r ON r.glob_id = l.glob_id AND r.rec_activ = 0 AND r.kod_acc = 1
        WHERE
            l.rec_activ = 0 AND
            l.kod_sost > 2 and
            l.date_vyd_d BETWEEN TO_DATE('{date_range[0]}', 'YYYY-MM-DD') AND TO_DATE('{date_range[1]}', 'YYYY-MM-DD')
        """
        cursor.execute(query)
        result = cursor.fetchall()

        df = pd.DataFrame(result, columns=[desc[0] for desc in cursor.description])
    
        sql_connection = get_mssql_connection()
        sql_cursor = sql_connection.cursor()
        
        for _, row in df.iterrows():
            insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in range(len(df.columns))])})"
            sql_cursor.execute(insert_query, tuple(row))
        sql_connection.commit()
        
    finally:
        if connection is not None:
            connection.close()
        if sql_connection is not None:
            sql_connection.close()

    return f"Finished writing data for range {date_range}"