from connections import get_mongo_client, get_sql_server_connection
import pandas as pd


def max_number_find():
    conn = get_sql_server_connection()
    cursor = conn.cursor()

    # Check if table exists, if not create it
    check_table_query = f"""select max(number) from contingent_liabilities_task1_katm077;
    """
    cursor.execute(check_table_query)

    # Fetch the result (the single number)
    result = cursor.fetchone()

    # result will be a tuple with one element, so you can extract the number like this:
    max_number = int(result[0] if result else None)
    return max_number


# max_num = max_number_find()

# Function to insert DataFrame into MSSQL in chunks
def insert_into_mssql(df, table_name):
    conn = get_sql_server_connection()
    cursor = conn.cursor()

    # Check if table exists, if not create it
    check_table_query = f"""
    IF OBJECT_ID(N'{table_name}', 'U') IS NULL
    BEGIN
        CREATE TABLE {table_name}  (
    {', '.join([f'[{col}] NVARCHAR(1000)' for col in df.columns])}
);
    END;"""
    cursor.execute(check_table_query)
    conn.commit()

    # Insert the data from DataFrame into the table
    for index, row in df.iterrows():
        try:
            insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['?' for _ in range(len(df.columns))])})"
            values = [str(val) for val in row]
            cursor.execute(insert_query, tuple(values))
        except Exception as e:
            print(f"Error inserting row {index}: {e}")

    conn.commit()
    cursor.close()
    conn.close()


client = get_mongo_client()

# Select the database and collection
db = client['task']
task_collection = db['task']

# Define the query
query = {
    'data.katm_077.return.data.contingent_liabilities.contingent_liability': {'$exists': True}
    ,'number': {'$gt': 1243100, '$lt': 1243200}
}

# Define the projection
projection = {
    'number': 1,
    'data.katm_077.return.data.contingent_liabilities.contingent_liability': 1
}

# Fetch a single document
doc = task_collection.find(query, projection)
for document in doc:
    # Prepare data for Pandas if the document exists
    rows = []
    if document:
        _id = str(document.get('_id'))  # Convert `_id` to string
        number = document.get('number')  # Extract the `number` field
        contracts = document.get('data', {}).get('katm_077', {}).get('return', {}).get('data', {}).get('contingent_liabilities', {}).get('contingent_liability', [])
        if isinstance(contracts, dict):
            contracts = [contracts]
        elif contracts is None:
            contracts = []
        for contract in contracts:
            # Flatten the contract details and include the `number` field
            row = {'_id': _id, 'number': number, **contract}
            rows.append(row)

        df = pd.DataFrame(rows)

    # Function to clean nested columns and prepare final DataFrame
    def clean_nested_columns(df):
        # Remove columns with lists or dictionaries
        for col in df.columns:
            if any(isinstance(val, (list, dict)) for val in df[col]):
                df = df.drop(columns=[col])
        return df

    # Function to write the last loaded index to the checkpoint file

    # Function to create an empty DataFrame with specified columns
    def create_empty_df_with_columns(columns):
        return pd.DataFrame(columns=columns)

    # Function to load contract columns from a text file
    def load_contract_columns(file_path):
        with open(file_path, 'r') as file:
            columns = file.read().splitlines()  # Read each line as a column name
        return columns

    # Function to map `dff` values to a DataFrame with specified columns
    def map_dff_to_contract_columns(dff, contract_columns):
        # Ensure '_id' and 'number' are in the contract_columns list
        essential_columns = ['_id', 'number']
        for col in essential_columns:
            if col not in contract_columns:
                contract_columns.append(col)

        # Create an empty DataFrame with the contract columns
        final_df = create_empty_df_with_columns(contract_columns)

        # Fill in the columns of `final_df` with values from `dff`
        for col in final_df.columns:
            if col in dff.columns:
                final_df[col] = dff[col]  # Copy the values from `dff`
            else:
                final_df[col] = None  # Set the column to None if not in `dff`

        return final_df

    guarantees_columns_file = 'katm_077_contingent_liabilities_columns.txt'

    contract_columns = load_contract_columns(guarantees_columns_file)

    final_df = map_dff_to_contract_columns(df, contract_columns)

    # Attempt insertion and handle SQL errors to skip problematic documents

    insert_into_mssql(final_df, 'bronze.katm_077_contingent_liabilities')
