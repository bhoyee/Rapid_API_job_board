from etl import load_to_s3, read_transform_files_from_s3, load_to_redshift
from util import get_redshift_connection,generate_schema,execute_sql
import boto3

# Create a boto3 s3 client for bucket operations
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

def main():
    table_name = 'job_logs'
    conn = get_redshift_connection()

    load_to_s3()
    file_name,data = read_transform_files_from_s3()
    output_query = generate_schema(data, table_name)
    execute_sql(output_query, conn)

    load_to_redshift(table_name,file_name)

main()
