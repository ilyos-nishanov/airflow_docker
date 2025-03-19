def Mongo_extract_number_filter(columns, column_names, gt, lt=99999999):
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure
    from datetime import datetime
    import pandas as pd
    from tqdm import tqdm
    from datetime import datetime, timedelta
    import numpy as np

    connection_string = "mongodb://Bekhzod.Maksudov:pF6rdbSB@172.17.39.13:27017/?authSource=admin"

    try:
        # Connect to MongoDB
        client = MongoClient(connection_string)
        
        # Check if the connection was successful
        client.admin.command('ping')
        print("Successfully connected to MongoDB")
        
        # Select the database and collection
        db = client['task']
        task_collection = db['task']
        
        # Define the query

        query = {
            'number': {'$gt': gt, '$lte': lt}
        }

        # Define the projection (fields to include)
        projection = {field: 1 for field in columns}
        
        # Fetch all documents with the given query and projection
        documents = task_collection.find(query, projection)

        # Create an empty list to store normalized documents
        normalized_docs = []

        # Loop through documents and normalize
        for doc in tqdm(documents, desc="Normalizing documents"):
            normalized_doc = pd.json_normalize(doc)  # Normalize the document to flatten nested structure
            normalized_docs.append(normalized_doc)
        
        # Combine all normalized documents into a single DataFrame
        df = pd.concat(normalized_docs, ignore_index=True)
        # df['created'] = df['created'] + pd.to_timedelta(5, unit='h')
    except ConnectionFailure as e:
        print(f"Could not connect to MongoDB: {e}")


    for column in columns: 
        if column in list(df.columns):
            continue
        else:
            df[column] = np.nan
    df = df[columns]

    df.columns = column_names
    return df 

def max_app_id_find(table_name, id_column):
    import pyodbc
    from bson import ObjectId
    from pymongo import MongoClient
    import pandas as pd
    # Connection details
    driver = 'ODBC Driver 17 for SQL Server'
    server = '172.17.17.22,54312' 
    database = 'RISKDB'
    username = 'risk_technology_dev'
    password = 'tTcnjl6T'
    
    conn = pyodbc.connect(
        f"Driver={{{driver}}};"
        f"Server={server};"
        f"Database={database};"
        f"UID={username};"
        f"PWD={password};"
    )

    cursor = conn.cursor()
 
    # Check if table exists, if not create it
    check_table_query = f"""SELECT REPLACE(max(CAST({id_column} as BIGINT)), ',', '') FROM {table_name};
    """
    cursor.execute(check_table_query)
 
    # Fetch the result (the single number)
    result = cursor.fetchone()
 
    # result will be a tuple with one element, so you can extract the number like this:
    max_number = int(result[0] if result else None)
    return max_number

columns = ['type', 'number', 'created', 'status',
         'request.clientId', "request.requestId",
         "request.credProgId", "request.amount",
         "asbt.status", "asbt.statusName",
         "data.asbt_service_client.result.data.fio", "data.asbt_service_client.result.data.phone",
         "data.asbt_service_client.result.data.pinfl", "data.asbt_service_client.result.data.pasSer",
         "data.asbt_service_client.result.data.pasNum", "data.asbt_service_client.result.data.dateBirth",
         "data.asbt_service_client.result.data.postAdr", "data._salary.result.data.avgByMonthReal", "data._salary.result.data.salary_from",
         "data._salary.result.data.lastEmployer.inn", "data._salary.result.data.lastEmployer.title", "data.katm_077.result.data.today_payment",
         "result.is_ok", "result.error.errmsg", "result.error.message", 'data.katm_077.result.data.today_payment',
         "data.katm_077.result.data.loan_summa", "data.crm_light_check_stop_factor.result.data.credit.avg_remainder",
        "data.crm_light_check_stop_factor.result.data.credit.annuitet_cash", "data.asbt_service_client.result.data.kodOblAddress", "request.rate",
        "request.term", "data.katm_077.result.hint", "data.asbt_credit_list.result.hint", "duration_sec", "data.uzasbo_working.return.Success",
        "response.data.summaLimit", "response.data.segment", "data.katm_077.return.data.scorring.scoring_grade",
        "data.asbt_service_client.return.object.kodOblAddress", "data.asbt_service_client.return.object.kodRegAddress",
        "data._task_check_limitV2.payload.Current_cash_outstanding", "data._task_check_limitV2.payload.Current_pmt", "data._task_check_limitV2.payload.Final_Limit",
        "data._task_check_limitV2.payload.IR", "data._task_check_limitV2.payload.LTI_cutoff", "data._task_check_limitV2.payload.Limit_by_LTI",
        "data._task_check_limitV2.payload.Limit_by_Out", "data._task_check_limitV2.payload.Limit_by_PTI_CB", "data._task_check_limitV2.payload.Limit_by_PTI_FACT",
        "data._task_check_limitV2.payload.Max_PTI_pmt", "data._task_check_limitV2.payload.Max_product_Limit", "data._task_check_limitV2.payload.PTI_cutoff",
        "data._task_check_limitV2.payload.Term", "result.data.prc", "result.data.percentBySegmentAndIncome", "data.katm_333.duration_sec", "data.katm_333.is_from_cache",
        "data.katm_077.result.data.summa_other_mikrozaym", "data._salaryV2.payload.segment", "data._salaryV2.payload.salarySegment", "data._salaryV2.payload.Salary", "request.statement", "data.asbt_credit_list.result.current_dlq_IB",
    "data.katm_077.result.data.current_dlq_bureau",
    "data.katm_077.result.data.current_dlq_total", 'asbt.globId',"data._task_check_limitV2.payload.Current_cash_outstanding_IB",
    "data._task_check_limitV2.payload.Current_cash_outstanding_bureau",
    "data.katm_077.result.data.bureau_segment"]
column_names = columns
gt = max_app_id_find('onl_app', 'app_id')
print(gt)
df = Mongo_extract_number_filter(columns, column_names, gt=gt)
import pandas as pd
import numpy as np
import pyodbc
from tqdm import tqdm
from datetime import datetime, timedelta


df['created'] = df['created'] + pd.to_timedelta(5, unit='h')


for column in columns: 
    if column in list(df.columns):
        continue
    else:
        df[column] = np.nan
# df = df[['type', 'number', 'created', 'status',
#          'request.clientId', "request.requestId",
#          "request.credProgId", "request.amount",
#          "asbt.status", "asbt.statusName",
#          "data.asbt_service_client.result.data.fio", "data.asbt_service_client.result.data.phone",
#          "data.asbt_service_client.result.data.pinfl", "data.asbt_service_client.result.data.pasSer",
#          "data.asbt_service_client.result.data.pasNum", "data.asbt_service_client.result.data.dateBirth",
#          "data.asbt_service_client.result.data.postAdr", "data._salary.result.data.avgByMonthReal", "data._salary.result.data.salary_from",
#          "data._salary.result.data.lastEmployer.inn", "data._salary.result.data.lastEmployer.title",
#          "result.is_ok", "result.error.errmsg", "result.error.message", 'data.katm_077.result.data.today_payment',
#          "data.katm_077.result.data.loan_summa", "data.crm_light_check_stop_factor.result.data.credit.avg_remainder",
#         "data.crm_light_check_stop_factor.result.data.credit.annuitet_cash", "data.asbt_service_client.result.data.kodOblAddress", "request.rate",
#         "request.term", "data.katm_077.result.hint", "data.asbt_credit_list.result.hint", "duration_sec", "data.uzasbo_working.return.Success",
#         "response.data.summaLimit", "response.data.segment", "data.katm_077.return.data.scorring.scoring_grade",
#         "data.asbt_service_client.return.object.kodOblAddress", "data.asbt_service_client.return.object.kodRegAddress",
#         "data._task_check_limitV2.payload.Current_cash_outstanding", "data._task_check_limitV2.payload.Current_pmt", "data._task_check_limitV2.payload.Final_Limit",
#         "data._task_check_limitV2.payload.IR", "data._task_check_limitV2.payload.LTI_cutoff", "data._task_check_limitV2.payload.Limit_by_LTI",
#         "data._task_check_limitV2.payload.Limit_by_Out", "data._task_check_limitV2.payload.Limit_by_PTI_CB", "data._task_check_limitV2.payload.Limit_by_PTI_FACT",
#         "data._task_check_limitV2.payload.Max_PTI_pmt", "data._task_check_limitV2.payload.Max_product_Limit", "data._task_check_limitV2.payload.PTI_cutoff",
#         "data._task_check_limitV2.payload.Term", "result.data.prc", "result.data.percentBySegmentAndIncome", "data.katm_333.duration_sec", "data.katm_333.is_from_cache",
#         "data.katm_077.result.data.summa_other_mikrozaym", "data._salaryV2.payload.segment", "data._salaryV2.payload.salarySegment", "data._salaryV2.payload.Salary", "request.statement"]]

df.columns = ['loan_type', 'app_id', 'app_dt', 'status', 'client_id', 'request_id',
       'credprogid', 'loan_amt', 'asbt_status', 'asbt_comment', 'fio', 'phone',
       'pinfl', 'id_series', 'id_number', 'birth_date', 'address',
       'avg_salary', 'salary_source', 'employer_INN', 'employer_name',
       'avg_monthly_payment', 'result', 'error', 'error_detail', 'avg_payment', 'overall_debt', 
        'avg_remainder', 'annuitet_cash', 'region_residence', 'Interest_rate', 'Term', 'katm_dpd_amt', 'asbt_dpd_amt', 'duration_sec',
              'uzasbo_working', 'summa_limit', 'segment', 'scoring_grade', 'kodOblAddress', 'kodRegAddress',  '_task_check_limitV2_Current_cash_outstanding',
'_task_check_limitV2_Current_pmt',
'_task_check_limitV2_Final_Limit',
'_task_check_limitV2_IR',
'_task_check_limitV2_LTI_cutoff',
'_task_check_limitV2_Limit_by_LTI',
'_task_check_limitV2_Limit_by_Out',
'_task_check_limitV2_Limit_by_PTI_CB',
'_task_check_limitV2_Limit_by_PTI_FACT',
'_task_check_limitV2_Max_PTI_pmt',
'_task_check_limitV2_Max_product_Limit',
'_task_check_limitV2_PTI_cutoff',
'_task_check_limitV2_Term',
'prc',
'percentBySegmentAndIncome',
'katm_333_duration_sec',
'katm_333_is_from_cache', 'summa_other_mikrozaym', 'payload_segment', 'payload_salarySegment', 'payload_Salary', "statement",
'current_dlq_IB', 'current_dlq_bureau', 'current_dlq_total', 'glob_id', 'Current_cash_outstanding_IB', 'Current_cash_outstanding_bureau', 'bureau_segment']

# Convert the 'date_column' to datetime format
df['app_dt'] = pd.to_datetime(df['app_dt'], format='%Y-%m-%d %H:%M:%S.%f')
df['app_dt'] = df['app_dt'] + timedelta(hours=5)

def transform_value(x):
    if pd.isna(x) or x == 0.0:
        return 0  # Replace NaN and 0.0 with 0
    try:
        x = float(x)  # Attempt to convert to float
        if x > 0:
            return 1  # Replace values greater than 0 with 1
        elif x == 0.0:
            return 0  # Replace 0 with 0
    except ValueError:
        pass  # If conversion fails, leave the value unchanged
    
    return x  # Leave other values unchanged

df['asbt_status'] = df['asbt_status'].apply(transform_value)
df['id_number'] = df['id_number'].astype('object')
def modify_value(value):
    if '.' in str(value):
        return str(value)[:-11]  # Remove last 10 characters
    return value

# Apply the function to the column
df['client_id'] = df['client_id'].apply(modify_value)
df['client_id'] = df['client_id'].astype('int64')
df['result'] = df['result'].fillna(0)
df['result'] = df['result'].astype('int64')
df['birth_date'] = pd.to_datetime(df['birth_date'], format='%Y-%m-%d %H:%M:%S.%f')
df['loan_amt'] = df['loan_amt'].fillna(0)
df['loan_amt'] = df['loan_amt'].astype('int64')
df['asbt_status'] = df['asbt_status'].astype('int64')



df['date'] = df['app_dt'].dt.strftime('%Y-%m-%d')
df['date'] = pd.to_datetime(df['date'])

df['loan_amt_s'] = df['loan_amt'] // 100
df['month'] = df['date'].dt.strftime('%YM%m')
df_filtered = df.sort_values(by=['asbt_status', 'result', 'app_dt'], ascending=False).groupby(['date', 'client_id']).first().reset_index()
df_filtered['unique_flag'] = 1
df = pd.merge(left = df, right = df_filtered[['app_id', 'unique_flag']], left_on = ['app_id'], right_on = ['app_id'], how = 'left')
df['unique_flag'] = df['unique_flag'].fillna(0)
df['unique_flag'] = df['unique_flag'].astype('int64')

df['asbt_dpd_amt'] = df['asbt_dpd_amt'].where(df['asbt_dpd_amt'].str.startswith('Просрочка'), np.nan)
df['katm_dpd_amt'] = df['katm_dpd_amt'].where(df['katm_dpd_amt'].str.startswith('Просрочка'), np.nan)

df['flag_asbt_dpd'] = df['asbt_dpd_amt'].apply(lambda x: 0 if pd.isna(x) else 1)
df['flag_katm_dpd'] = df['katm_dpd_amt'].apply(lambda x: 0 if pd.isna(x) else 1)

df['asbt_dpd_amt'] = df['asbt_dpd_amt'].apply(lambda x: ' '.join(str(x).split()[1:]))
df['katm_dpd_amt'] = df['katm_dpd_amt'].apply(lambda x: ' '.join(str(x).split()[1:]))



df = df[['loan_type', 'app_id', 'app_dt', 'status', 'client_id', 'request_id',
       'credprogid', 'loan_amt', 'asbt_status', 'asbt_comment', 'fio', 'phone',
       'pinfl', 'id_series', 'id_number', 'birth_date', 'address',
       'avg_salary', 'salary_source', 'employer_INN', 'employer_name',
       'avg_monthly_payment', 'result', 'error', 'error_detail', 'date',
       'loan_amt_s', 'month', 'unique_flag', 'Interest_rate', 'Term', 'asbt_dpd_amt',
        'katm_dpd_amt', 'flag_asbt_dpd', 'flag_katm_dpd', 'avg_payment',
       'overall_debt', 'avg_remainder', 'annuitet_cash', 'region_residence', 'duration_sec', 'uzasbo_working', 'summa_limit', 
        'segment', 'scoring_grade', 'kodOblAddress', 'kodRegAddress',  '_task_check_limitV2_Current_cash_outstanding',
'_task_check_limitV2_Current_pmt',
'_task_check_limitV2_Final_Limit',
'_task_check_limitV2_IR',
'_task_check_limitV2_LTI_cutoff',
'_task_check_limitV2_Limit_by_LTI',
'_task_check_limitV2_Limit_by_Out',
'_task_check_limitV2_Limit_by_PTI_CB',
'_task_check_limitV2_Limit_by_PTI_FACT',
'_task_check_limitV2_Max_PTI_pmt',
'_task_check_limitV2_Max_product_Limit',
'_task_check_limitV2_PTI_cutoff',
'_task_check_limitV2_Term',
'prc',
'percentBySegmentAndIncome',
'katm_333_duration_sec',
'katm_333_is_from_cache', 'summa_other_mikrozaym', 'payload_segment', 'payload_salarySegment', 'payload_Salary', 'statement',
'current_dlq_IB', 'current_dlq_bureau', 'current_dlq_total', 'glob_id', 'Current_cash_outstanding_IB', 'Current_cash_outstanding_bureau', 'bureau_segment']]

print(df)

driver = 'ODBC Driver 17 for SQL Server'
server = '172.17.17.22,54312' 
database = 'RISKDB'
username = 'risk_technology_dev'
password = 'tTcnjl6T'

# Establish the connection
connection_mssql = pyodbc.connect(
    "Driver={" + driver + "};"
    "Server=" + server + ";"
    "Database=" + database + ";"
    "UID=" + username + ";"
    "PWD=" + password + ";"
)

# Verify if connection was successful
print("Connection successful!")

cursor_mssql = connection_mssql.cursor()

# Table name where data will be inserted
table_name = 'onl_app'

# Wrap the insertion process with tqdm to show progress
for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Inserting rows", unit="row"):
    # Modify the INSERT query to handle special characters and Unicode
    insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in range(len(df.columns))])})"

    # Convert values to strings to handle special characters
    values = [str(val) for val in row]

    # Execute the insert query
    cursor_mssql.execute(insert_query, tuple(values))

# Commit the transaction after inserting all rows
connection_mssql.commit()

# Close the cursor and connection
cursor_mssql.close()
connection_mssql.close()

