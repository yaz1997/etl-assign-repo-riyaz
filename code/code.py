# Data taken from API and cleaned and loaded to destination bucket

import boto3
import pandas as pd
import io

def lambda_handler(event, context):
    # Initialize S3 client
    s3 = boto3.client('s3')

    # Define the source and destination bucket and key
    source_bucket = 'apprentice-training-data-riyaz-dev'
    source_key = 'games_data.csv'

    destination_bucket = 'apprentice-training-data-riyaz1-dev'
    destination_key = 'cleaned_games_data.csv'
    
    # Download CSV from source bucket
    response = s3.get_object(Bucket=source_bucket, Key=source_key)
    csv_content = response['Body'].read()

    # Perform data cleaning using pandas
    df = pd.read_csv(io.BytesIO(csv_content))
    
  
    # Perform your data cleaning operations on the dataframe
    # Remove duplicates in publisher column
    df.drop_duplicates(subset=['publisher'], inplace=True)
    
    #Remove games before '2014-01-01'
    df['release_date'] = pd.to_datetime(df['release_date'])
    threshold_date = pd.to_datetime('2014-01-01')
    cleaned_df = df.loc[df['release_date'] >= threshold_date]
   

    # Convert the cleaned dataframe back to CSV content
    cleaned_csv_content = df.to_csv(index=False)

    # Upload cleaned CSV to destination bucket
    s3.put_object(Bucket=destination_bucket, Key=destination_key, Body=cleaned_csv_content)

    return {
        'statusCode': 200,
        'body': 'Data cleaned and stored successfully!'
    }





# Loaded the cleaned data from destination bucket to DBeaver 

import psycopg2
import boto3
import pandas as pd
import io
import csv

from io import StringIO

def lambda_handler(event, context):
    # Initialize S3 client
    s3 = boto3.client('s3')

    bucket_name = 'apprentice-training-data-riyaz1-dev'
    object_key = 'cleaned_games_data.csv'

    db_params = {
        "host": "apprentice-training-2023-rds.cth7tqaptja4.us-west-1.rds.amazonaws.com",
        "database": "postgres",
        "user": "postgres",
        "password": "hello123"
        }

    conn = psycopg2.connect(**db_params)
    
    cursor = conn.cursor()
    
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    data = response['Body'].read().decode('utf-8')

    # Parse data as CSV
    csv_data = StringIO(data)
    csv_reader = csv.reader(csv_data)
    next(csv_reader)  
    
    cursor = conn.cursor()

    values_to_insert = []
    for row in csv_reader:
        
        id = row[0]
        title = row[1]
        thumbnail = row[2]
        short_description = row[3]
        game_url = row[4]
        genre = row[5]
        platform = row[6]
        publisher = row[7]
        developer = row[8]
        release_date = row[9]
        freetogame_profile_url = row[10]
        
        values_to_insert.append((id, title, thumbnail, short_description, game_url, genre, platform, publisher, developer, release_date, freetogame_profile_url))
        
        

    placeholders = ', '.join(['%s'] * len(values_to_insert[0]))
    sql = f"INSERT INTO riyaz_etl_games_data VALUES ({placeholders})"
    
    cursor.executemany(sql, values_to_insert)
    
    conn.commit()
    cursor.close()
    conn.close()
    
    return {
         'statusCode': 200,
         'body': 'Data transferred successfully'
        }

#check to commmit