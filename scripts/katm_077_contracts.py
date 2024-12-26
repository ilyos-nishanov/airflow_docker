import pandas as pd
from connections import get_mongo_client
from mssql import max_number_find, insert_into_mssql

max_num = max_number_find()
client = get_mongo_client()

# Select the database and collection
db = client['task']
task_collection = db['task']

# Define the query
query = {
    'data.katm_077.return.data.contracts.contract': {'$exists': True},
    # 'number': {'$gt': max_num}  # max_date= 'select max(number) from table_name'
}

# Adjusted projection to include the entire contracts list
projection = {
    'number': 1,
    'data.katm_077.return.data.contracts.contract': 1
}

# Fetch the documents that match the query
docs = task_collection.find(query, projection)

for document in docs:
    rows = []
    if document:
        _id = str(document.get('_id'))  # Convert `_id` to string
        number = document.get('number')  # Extract the `number` field
        contracts = document.get('data', {}).get('katm_077', {}).get('return', {}).get('data', {}).get('contracts', {}).get('contract', [])

        if isinstance(contracts, dict):
            contracts = [contracts]

        for contract in contracts:
            # Flatten the contract details and include the `_id` and `number` fields
            row = {'_id': _id, 'number': number, **contract}
            rows.append(row)

    # Create a DataFrame from the rows
    df = pd.DataFrame(rows)

    # Function to create an empty DataFrame with specified columns
    def create_empty_df_with_columns(columns):
        return pd.DataFrame(columns=columns)

    # Function to load contract columns from a text file
    def load_contract_columns(file_path):
        with open(file_path, 'r') as file:
            columns = file.read().splitlines()  # Read each line as a column name
        return columns

    # Function to map `df` values to a DataFrame with specified columns
    def map_df_to_contract_columns(df, contract_columns):
        # Ensure '_id' and 'number' are in the contract_columns list
        essential_columns = ['_id', 'number']
        for col in essential_columns:
            if col not in contract_columns:
                contract_columns.append(col)

        # Create an empty DataFrame with the contract columns
        final_df = create_empty_df_with_columns(contract_columns)

        # Fill in the columns of `final_df` with values from `df`
        for col in final_df.columns:
            if col in df.columns:
                final_df[col] = df[col]  # Copy the values from `df`
            else:
                final_df[col] = None  # Set the column to None if not in `df`

        return final_df

    # Load the contract columns from the file
    contract_columns_file = 'katm077_contracts_columns.txt'
    contract_columns = load_contract_columns(contract_columns_file)

    # Map the DataFrame to match the contract columns
    final_df = map_df_to_contract_columns(df, contract_columns)

    # Insert the final DataFrame into MSSQL
    insert_into_mssql(final_df, 'bronze.katm_077_contracts')
