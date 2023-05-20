from distutils import config
from io import StringIO
import io
import requests
import json
from datetime import datetime, timedelta
import os
import dotenv
import boto3
import pandas as pd
from util import get_redshift_connection,execute_sql, list_files_in_folder,move_files_to_processed_folder


dotenv.load_dotenv()

# Load the RAPIDAPI API key from the .env file
RAPIDAPI_API_KEY = os.environ.get('RAPIDAPI_API_KEY')

s3 = boto3.resource('s3')
bucket_name = 'rapid-job-search-data'
transformed_bucket_name = 'by-transformed-jobs-data'
transformed_path_name = 'by-transformed-jobs'

# Create a directory for storing the extracted JSON files
os.makedirs(os.path, exist_ok=True)

# Define the URL of the RapidAPI job search API
url = "https://jsearch.p.rapidapi.com/search"

#created an extract function from the api
def extract_from_API_(url,countries,jobs):
    all_data = pd.DataFrame()
    for i in countries:
        for j in jobs:
            querystring = {"query":f"{i}, {j}","page":"1","num_pages":"1"}
            headers = {
                    "X-RapidAPI-Key": RAPIDAPI_API_KEY,
                    "X-RapidAPI-Host": "jsearch.p.rapidapi.com"}
            response = requests.get(url, headers=headers, params=querystring)
            response = response.json()
            data = response.get('data')
            data = pd.DataFrame(data)
            all_data = pd.concat([all_data, data])

    return all_data

def load_to_s3():
    countries = ['USA', 'UK', 'Canada']
    jobs = ['Data engineer', 'Data Analyst']
    url = url

    data = extract_from_API_(url,countries,jobs)
    
    file_name = f"{datetime.now().strftime('%Y-%m-%d-%H-%M')}"
    file_path = os.path.join(os.path, file_name)

    # Save the JSON data to a local file
    data.to_json(file_path, orient='records')

    csv_buffer = StringIO()
    data.reset_index(drop=True, inplace=True)
    data.to_json(csv_buffer,orient='columns')
    csv_str = csv_buffer.getvalue()


    s3.put_object(Bucket=bucket_name, Key = f'{os.path}/{file_name}', Body=csv_str)

    print("file loaded successfully to the s3 bucket")

