# from snowflake.snowpark.session import Session, FileOperation
# import pandas as pd
# import boto3
# import os
 
# csv_file="export.csv"
# xl_file="import.xlsx"
# s3_file="s3://poc-spreadsheetsf/USNationalParks.xlsx"
 
# # Create Session object
# def create_session_object():
#     # Snowflake connection parameters
#     connection_parameters = {
#         "account": "redpill",
#         "user": "tatumbrannan",
#         "password": "Doodletoot1",
#         "role": "sysadmin",
#         "warehouse": "COMPUTE_WH",
#         "database": "snowflake_101",
#         "schema": "development"
#     }
#     session = Session.builder.configs(connection_parameters).create()
#     return session
 
# def convert_excel_to_csv():
#     read_file = pd.read_excel(xl_file)
 
#     #remove file if it exists 
#     try:
#         os.remove(csv_file)
#     except OSError:
#         pass
 
#     #write to csv format
#     read_file.to_csv (csv_file, 
#                   index = None,
#                   header = True) 
 
# def load_data(session):
#     # Create internal stage if it does not exists
#     session.sql("create or replace stage demo ").collect()
    
#     #Upload file to stage
#     FileOperation(session).put(csv_file, '@demo/export.csv')
    
#     # Create or replace snowflake table
#     session.sql("create or replace table national_parks(Country varchar, Abreviation varchar, Capital_City varchar, Continent varchar)").collect()
#     # Create or replace snowflake table
    
#     #select warehouse
#     session.sql("use warehouse demo_wh").collect()
    
#     #load table from stage
#     session.sql("copy into national_parks from @demo file_format= (type = csv field_delimiter=',' skip_header=1)").collect()
    
#     #drop stage
#     session.sql("drop stage demo").collect()
    
    
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
#         print("Error:", e)
 
# if __name__ == "__main__":
#     if not os.path.exists(xl_file):
#         get_file()
#     session = create_session_object()
#     convert_excel_to_csv()
#     load_data(session)
#     session.close()


import pandas as pd
from snowflake.snowpark.session import Session, FileOperation
import boto3
import os

csv_file = "export.csv"
xl_file = "import.xlsx"
s3_file = "s3://poc-spreadsheetsf/USNationalParks.xlsx"


# Create Session object
def create_session_object():
    # Snowflake connection parameters
    connection_parameters = {
        "account": "redpill",
        "user": "tatumbrannan",
        "password": "Doodletoot1",
        "role": "sysadmin",
        "warehouse": "COMPUTE_WH",
        "database": "snowflake_101",
        "schema": "development"
    }
    session = Session.builder.configs(connection_parameters).create()
    return session


def main():
    if not os.path.exists(xl_file):
        get_file()

    # Load all sheets from the Excel file into a dictionary of DataFrames
    xls = pd.ExcelFile(xl_file)
    all_sheets = pd.read_excel(xls, sheet_name=None)

    cleaned_data = {}

    # Clean up each DataFrame in the dictionary
    for sheet_name, df in all_sheets.items():
        # Remove empty rows and columns
        df = df.dropna(how='all')
        df = df.dropna(axis=1, how='all')
        cleaned_data[sheet_name] = df

    if cleaned_data:
        # Concatenate all DataFrames into one
        combined_df = pd.concat(cleaned_data.values(), ignore_index=True)
        
        # Convert combined DataFrame to CSV
        csv_filename = "combined_data.csv"
        combined_df.to_csv(csv_filename, index=False)
        
        # Upload CSV to Snowflake
        upload_csv_to_snowflake(csv_filename)
    else:
        print("No data was cleaned or an error occurred.")


def upload_csv_to_snowflake(csv_filename):
    session = create_session_object()
    # Create internal stage if it does not exist
    session.sql("create or replace stage demo").collect()

    # Upload file to stage
    FileOperation(session).put(csv_filename, f"@demo/{csv_filename}")

    # Create or replace Snowflake table with column definitions
    table_name = "excel_to_csv"
    session.sql(f"create or replace table {table_name} (Country varchar, Abreviation varchar, Capital_City varchar, Continent varchar)").collect()

    # Select warehouse
    session.sql("use warehouse demo_wh").collect()

    # Load table from stage
    session.sql(f"""copy into {table_name} from @demo/{csv_filename} 
        file_format=(type = csv field_delimiter=',' skip_header=1 
        empty_field_as_null=true)""").collect()

    # Drop stage
    session.sql("drop stage demo").collect()

    session.close()

def get_file():
    # Initialize the S3 client
    s3 = boto3.client('s3')

    # Specify the bucket name and the object key (file name)
    bucket_name = 'poc-spreadsheetsf'
    object_key = 'USNationalParks.xlsx'

    try:
        # Send a GET request to retrieve the object from S3
        response = s3.get_object(
            Bucket=bucket_name,
            Key=object_key
        )

        object_content = response['Body'].read()

        with open(xl_file, 'wb') as f:
            f.write(object_content)

        print("Object downloaded successfully.")

    except Exception as e:
        print("Error creating Snowflake table:", e)


if __name__ == "__main__":
    main()
