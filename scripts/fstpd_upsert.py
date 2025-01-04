
from datetime import datetime
from fstpd_utils import initialize_sql_table, get_date_range, write_to_sql, upsert
from fetch_data import fetch_oracle_data

start_time = datetime.now()
print(f"Start time: {start_time}")

table_name = 'BRONZE.FSTPD_UPSERT_IT'
columns = ['DATE_VYD_D', 'GLOB_ID', 'K_VID_CRED', 'FPD', 'SPD', 'TPD', 'DATE_MODIFIED']
initialize_sql_table(table_name, columns)

products = '24, 34, 32'
start_date, end_date = get_date_range()

dff = fetch_oracle_data(products, start_date, end_date)
write_to_sql(dff, table_name)

upsert_query = f"""

        MERGE INTO bronze.fstpd AS target
        USING (SELECT * FROM bronze.fstpd_upsert_it) AS source (date_vyd_d, glob_id, k_vid_cred, fpd, spd, tpd, date_modified)
        ON target.glob_id = source.GLOB_ID
        WHEN MATCHED THEN
            UPDATE SET 
                fpd = source.fpd, 
                spd = source.spd, 
                tpd = source.tpd, 
                date_modified = source.date_modified
        WHEN NOT MATCHED THEN
            INSERT
            VALUES (source.date_vyd_d, source.GLOB_ID, source.k_vid_cred, source.fpd, source.spd, source.tpd, source.date_modified);


"""
upsert(upsert_query)
end_time = datetime.now()
print(f"End time: {end_time}")