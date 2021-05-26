import json
import boto3
import csv
import codecs
import os


s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')

bucket = 'ca-new-bucket'
key = 'testfile.csv'
tableName = os.environ['TABLE_NAME']

def lambda_handler(event, context):

    data = s3.Object(bucket, key).get()['Body']
    batch_size = 100
    batch = []
    
    for row in csv.DictReader(codecs.getreader('utf-8')(data)):
      if len(batch) >= batch_size:
         write_to_dynamo(batch)
         batch.clear()

      batch.append(row)

    if batch:
      write_to_dynamo(batch)
    

    return {
        'statusCode': 200,
        'body': json.dumps("Dynamodb !!")
    }
    
def write_to_dynamo(rows):
   try:
      table = dynamodb.Table(tableName)
   except:
      print("Error loading DynamoDB table. Check if table was created correctly and environment variable.")

   try:
      with table.batch_writer() as batch:
         for i in range(len(rows)):
            batch.put_item(
               Item=rows[i]
            )
   except Exception as e:
      print("Error executing batch_writer", e);
