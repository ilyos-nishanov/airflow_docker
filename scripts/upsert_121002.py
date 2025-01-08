
from datetime import datetime
from my_utils import initialize_sql_table, get_date_range, write_to_sql, upsert
from fetch_data_121002 import fetch_oracle_data

print(f"Start time: {datetime.now()}")

table_name = 'BRONZE.121002'
columns = [
    "id_mfo", "kod_val", "client", "name_val", "acc_new", "acc_pros", "acc_ras", 
    "proc_ss", "proc_pr", "summa", "date_vyd_d", "date_pog_d", "dat_pogash", 
    "name_cel", "name_sost", "K_MAN", "GLOB_ID", "ID_APL", "n_kreditospos", 
    "inspect", "ss_status", "date_stop", "k_commis", "n_line", "val_com", 
    "pr_serv", "k_vid_cred", "k_type_obesp", "s_obesp", "id_cl", "pas_end", 
    "post_adr", "date_reg", "num_nalog", "n_gni", "ser_pas", "num_pas", 
    "n_sector", "n_ist", "n_obl", "n_reg", "date_birth", "liz_end", "dat_vyd", 
    "n_vid_cred", "n_obesp", "kol_work"
]
initialize_sql_table(table_name, columns)

dff = fetch_oracle_data()
write_to_sql(dff, table_name)

end_time = datetime.now()
print(f"End time: {end_time}")