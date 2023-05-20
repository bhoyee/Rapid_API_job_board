import psycopg2
from sqlalchemy import create_engine
import dotenv, os
from dotenv import dotenv_values
import pandas as pd
import psycopg2, boto3


# Get credentials from environment variable file
config = dotenv_values('.env')

# Create a boto3 s3 client for bucket operations
s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

def get_redshift_connection():
    user = config.get('USER')
    password = config.get('PASSWORD')
    host = config.get('HOST')
    database_name = config.get('DATABASE_NAME')
    port = config.get('PORT')
    conn = psycopg2.connect(f'postgresql://{user}:{password}@{host}:{port}/{database_name}')
    return conn

