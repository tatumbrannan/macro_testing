from snowflake.snowpark.session import Session, FileOperation
import pandas as pd
import boto3
import os
 
csv_file="export.csv"
xl_file="import.xlsx"
s3_file="s3://poc-spreadsheetsf/USNationalParks.xlsx"
 
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
 
def convert_excel_to_csv():
    read_file = pd.read_excel(xl_file)
 
    #remove file if it exists 
    try:
        os.remove(csv_file)
    except OSError:
        pass
 
    #write to csv format
    read_file.to_csv (csv_file, 
                  index = None,
                  header = True) 
 
def load_data(session):
    # Create internal stage if it does not exists
    session.sql("create or replace stage demo ").collect()
    
    #Upload file to stage
    FileOperation(session).put(csv_file, '@demo/export.csv')
    
    # Create or replace snowflake table
    session.sql("create or replace table national_parks(Country varchar, Abreviation varchar, Capital_City varchar, Continent varchar)").collect()
    # Create or replace snowflake table
    
    #select warehouse
    session.sql("use warehouse demo_wh").collect()
    
    #load table from stage
    session.sql("copy into national_parks from @demo file_format= (type = csv field_delimiter=',' skip_header=1)").collect()
    
    #drop stage
    session.sql("drop stage demo").collect()
    
    
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
        print("Error:", e)
 
if __name__ == "__main__":
    if not os.path.exists(xl_file):
        get_file()
    session = create_session_object()
    convert_excel_to_csv()
    load_data(session)
    session.close()
    
    
    
    
    