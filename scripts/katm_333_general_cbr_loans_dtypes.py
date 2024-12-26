import pandas as pd
from connections import get_mongo_client, get_sql_server_connection
from scripts.my_utils import convert_date, load_columns, map_dff_to_columns, max_number_find


loans_columns_file = 'katm_333_general_cbr_loans_fields.txt'
table_name = 'gold.katm_333_general_cbr_loans'
def insert_into_mssql(df, table_name=table_name):
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    for index, row in df.iterrows():
        try:
        # Insert data into SQL Server
            insert_query = f"""
                INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?)

            """
            values = (
            row['number'],                           # int
            row['client_id'],                        # int
            row.get('uuid', 'missing-uuid'),         # nvarchar
            convert_date(row['inf_confirm_date']),  # date
            row['currency'],                         # nvarchar
            row['relationship'],                     # int
            convert_date(row['open_date']),          # date
            convert_date(row['final_pmt_date']),     # date
            convert_date(row['fact_close_date']),    # date
            row['type'],                             # int
            row['credit_limit'],                     # float
            row['cur_to_base_limit'],                # float
            row['outstanding'],                      # float
            row['pmt_string_60m'],                   # nvarchar
            row['status'],                           # int
            row['next_pmt'],                         # float
            row['previous_pmt'],                     # float
            row['initial_pmt'],                      # float
            row['ttl_delq_7'],                       # int
            row['ttl_delq_8_29'],                    # int
            row['ttl_delq_30_59'],                   # int
            row['ttl_delq_60_89'],                   # int
            row['ttl_delq_90_119'],                  # int
            row['ttl_delq_120_plus'],                # int
            row['pmt_freq'],                         # int
            row['delq_balance'],                     # float
            row['max_delq_balance'],                 # float
            row['is_own'],                           # int
            row['current_delq'],                     # int
            convert_date(row['pmt_string_start']),   # date
            row['interest_rate'],                    # float
            convert_date(row['last_payment_date']),  # date
            convert_date(row['next_payment_date']),  # date
            row['curr_balance_amt'],                 # float
            row['collateral_code'],                  # int
            row['business_category'],                # int
            row['principal_outstanding'],            # float
            row['principal_past_due'],               # float
            row['int_outstanding'],                  # float
            row['int_past_due'],                     # float
            row['other_outstanding'],                # float
            row['other_past_due'],                   # float
            row['ensured_amount'],                   # float
            row['coborrowers_count'],                # int
            row['next_pmt_principal'],               # float
            convert_date(row['next_pmt_principal_date']),          # float
            row['next_pmt_interest'],                # float
            convert_date(row['next_pmt_interest_date']), # date
            row['principal_repayment_amount'],       # float
            row['principal_repaid_amount']           # float
            )
            cursor.execute(insert_query, values)
        except Exception as e:
            print(f"Error inserting row {index}: {e}")

    conn.commit()
    cursor.close()
    conn.close()
#############################################################################################################################################
client = get_mongo_client()
db = client['task']
task_collection = db['task']
# max_num = max_number_find(table_name)
query = {
    'data.katm_333.return.data.general_cbr.loans.loan': {'$exists': True}
    # , 'number': {'$gt': max_num} 
}
projection = {
    'number': 1, 
    'request.clientId': 1,
    'data.katm_333.return.data.general_cbr.loans.loan': 1

}
docs = task_collection.find(query, projection)
############################################################################################################################################
for document in docs:
    rows = []

    number = document.get('number')
    client_id = document.get('request', {}).get('clientId', {})
    loans = document.get('data', {}).get('katm_333', {}).get('return', {}).get('data', {}).get('general_cbr', {}).get('loans', {}).get('loan', [])       

    if isinstance(loans, dict):
        loans = [loans]

    for loan in loans:
        row = {'client_id': client_id, 'number': number, **loan}
        rows.append(row)
        
    df = pd.DataFrame(rows)
    
    loan_columns = load_columns(loans_columns_file)
    final_df = map_dff_to_columns(df, loan_columns)
    insert_into_mssql(final_df)
