# import pandas as pd
# from snowflake.snowpark.session import Session, FileOperation
# import boto3
# import os

# csv_file = "export.csv"
# xl_file = "import.xlsx"
# s3_file = "s3://poc-spreadsheetsf/USNationalParks.xlsx"


# # Create Session object
# def create_session_object():
#     # Snowflake connection parameters
#     connection_parameters = {
#         "account": "redpill",
#         "user": "pw",
#         "password": "Doodletoot1",
#         "role": "sysadmin",
#         "warehouse": "COMPUTE_WH",
#         "database": "snowflake_101",
#         "schema": "development"
#     }
#     session = Session.builder.configs(connection_parameters).create()
#     return session


# def main():
#     if not os.path.exists(xl_file):
#         get_file()

#     # Load all sheets from the Excel file into a dictionary of DataFrames
#     xls = pd.ExcelFile(xl_file)
#     all_sheets = pd.read_excel(xls, sheet_name=None)

#     cleaned_data = {}

#     # Clean up each DataFrame in the dictionary
#     for sheet_name, df in all_sheets.items():
#         # Remove empty rows and columns
#         df = df.dropna(how='all')
#         df = df.dropna(axis=1, how='all')
#         cleaned_data[sheet_name] = df

#     if cleaned_data:
#         # Concatenate all DataFrames into one
#         combined_df = pd.concat(cleaned_data.values(), ignore_index=True)
        
#         # Convert combined DataFrame to CSV
#         csv_filename = "combined_data.csv"
#         combined_df.to_csv(csv_filename, index=False)
        
#         # Upload CSV to Snowflake
#         upload_csv_to_snowflake(csv_filename)
#     else:
#         print("No data was cleaned or an error occurred.")


# def upload_csv_to_snowflake(csv_filename):
#     session = create_session_object()
#     # Create internal stage if it does not exist
#     session.sql("create or replace stage demo").collect()

#     # Upload file to stage
#     FileOperation(session).put(csv_filename, f"@demo/{csv_filename}")

#     # Create or replace Snowflake table with column definitions
#     table_name = "excel_to_csv"
#     session.sql(f"create or replace table {table_name} (Country varchar, Abreviation varchar, Capital_City varchar, Continent varchar)").collect()

#     # Select warehouse
#     session.sql("use warehouse demo_wh").collect()

#     # Load table from stage
#     session.sql(f"""copy into {table_name} from @demo/{csv_filename} 
#         file_format=(type = csv field_delimiter=',' skip_header=1 
#         empty_field_as_null=true)""").collect()

#     # Drop stage
#     session.sql("drop stage demo").collect()

#     session.close()

# def get_file():
#     # Initialize the S3 client
#     s3 = boto3.client('s3')

#     # Specify the bucket name and the object key (file name)
#     bucket_name = 'poc-spreadsheetsf'
#     object_key = 'USNationalParks.xlsx'

#     try:
#         # Send a GET request to retrieve the object from S3
#         response = s3.get_object(
#             Bucket=bucket_name,
#             Key=object_key
#         )

#         object_content = response['Body'].read()

#         with open(xl_file, 'wb') as f:
#             f.write(object_content)

#         print("Object downloaded successfully.")

#     except Exception as e:
#         print("Error creating Snowflake table:", e)


# if __name__ == "__main__":
#     main()


import os
import pandas as pd
import snowflake.connector
import re
import boto3
import tempfile

# Function to read CSV file with proper column names
def read_csv_with_column_names(file_path):
    # Read the CSV file with proper column names
    df = pd.read_csv(file_path, header=0)  # Assuming the first row contains column names
    return df

# Function to clean up table names
def clean_table_name(file_name):
    # Remove special characters from file name
    cleaned_name = re.sub(r'[^a-zA-Z0-9_]', '_', file_name)
    # Ensure the table name starts with a letter
    if cleaned_name[0].isdigit():
        cleaned_name = '_' + cleaned_name
    return cleaned_name

# Function to create Snowflake table from DataFrame
def create_table_from_dataframe(df, table_name, conn):
    # Sanitize column names
    df.columns = [re.sub(r'\W+', '_', col) for col in df.columns]

    # Replace NaN values with an empty string
    df.fillna('', inplace=True)

    # Debug: Print column names
    print("Sanitized column names:")
    print(df.columns)

    # Generate SQL statement to create table
    create_table_sql = f"CREATE OR REPLACE TABLE {table_name} ("
    for column in df.columns:
        create_table_sql += f'"{column}" VARCHAR,'  # Use double quotes for column names
    create_table_sql = create_table_sql[:-1] + ")"  # Remove trailing comma

    # Debug: Print SQL statement
    print("SQL statement for creating table:")
    print(create_table_sql)

    # Establish connection to Snowflake
    cursor = conn.cursor()
    try:
        cursor.execute(create_table_sql)
    except snowflake.connector.Error as e:
        print(f"Error executing SQL: {e}")

    # Copy data from DataFrame into Snowflake table
    try:
        placeholders = ','.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
        cursor.executemany(insert_query, df.values.tolist())
        conn.commit()  # Commit the transaction
    except snowflake.connector.Error as e:
        print(f"Error executing SQL: {e}")

    # Close cursor
    cursor.close()

# Function to convert Excel files to CSV
def excel_to_csv(bucket_name, csv_folder):
    # Initialize the S3 client
    s3 = boto3.client('s3')

    # List objects in the S3 bucket
    response = s3.list_objects_v2(Bucket=bucket_name)

    # Iterate through each object in the response
    for obj in response.get('Contents', []):
        # Extract the key (file path) of the object
        key = obj['Key']
        
        # Check if the object is an Excel file
        if key.endswith('.xlsx'):
            try:
                # Create a temporary file to store Excel content
                with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as temp_excel_file:
                    temp_excel_path = temp_excel_file.name
                
                # Download the Excel file from S3 to the temporary file
                s3.download_file(bucket_name, key, temp_excel_path)
                
                # Load the Excel file into a DataFrame
                df = pd.read_excel(temp_excel_path)
                
                # Convert DataFrame to CSV
                csv_filename = os.path.splitext(os.path.basename(key))[0] + '.csv'
                csv_file_path = os.path.join(csv_folder, csv_filename)
                df.to_csv(csv_file_path, index=False)
                print(f"Converted {key} to CSV and saved to {csv_file_path}")
            except Exception as e:
                print(f"Error processing {key}: {e}")

    # Check if response is truncated (indicating more objects)
    while response.get('IsTruncated', False):
        # Get next page of objects
        response = s3.list_objects_v2(Bucket=bucket_name, ContinuationToken=response['NextContinuationToken'])
        
        # Iterate through each object in the response
        for obj in response.get('Contents', []):
            # Extract the key (file path) of the object
            key = obj['Key']
            
            # Check if the object is an Excel file
            if key.endswith('.xlsx'):
                try:
                    # Create a temporary file to store Excel content
                    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as temp_excel_file:
                        temp_excel_path = temp_excel_file.name
                    
                    # Download the Excel file from S3 to the temporary file
                    s3.download_file(bucket_name, key, temp_excel_path)
                    
                    # Load the Excel file into a DataFrame
                    df = pd.read_excel(temp_excel_path)
                    
                    # Convert DataFrame to CSV
                    csv_filename = os.path.splitext(os.path.basename(key))[0] + '.csv'
                    csv_file_path = os.path.join(csv_folder, csv_filename)
                    df.to_csv(csv_file_path, index=False)
                    print(f"Converted {key} to CSV and saved to {csv_file_path}")
                except Exception as e:
                    print(f"Error processing {key}: {e}")

# Specify the directory containing CSV files
csv_directory = 'csv_folder'

# Specify the bucket name containing Excel files
bucket_name = 'poc-spreadsheetsf'

# Specify the local directory where you want to download CSV files
local_directory = 'csv_folder'

# Ensure the local directory exists, create it if necessary
if not os.path.exists(local_directory):
    os.makedirs(local_directory)

# Convert Excel files to CSV and save them in the local directory
excel_to_csv(bucket_name, local_directory)

# Establish connection to Snowflake
conn = snowflake.connector.connect(
        user='tatumbrannan',
        password='',
        account='redpill',
        warehouse='demo_wh',
        database='snowflake_101',
        schema='dbt_tbrannan'
    )


# Iterate over each CSV file and create Snowflake table
for file_name in os.listdir(csv_directory):
    if file_name.endswith('.csv'):
        # Construct the full file path
        csv_file_path = os.path.join(csv_directory, file_name)
        
        # Read CSV file with proper column names
        df = read_csv_with_column_names(csv_file_path)
        
        # Clean up table name
        table_name = clean_table_name(os.path.splitext(file_name)[0])

        # Create Snowflake table from DataFrame
        create_table_from_dataframe(df, table_name, conn)

# Close connection
conn.close()
