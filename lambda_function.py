from __future__ import print_function

# This is an AWS Lambda function written for Python 2.7 and triggered by CRON events
# to clean up logging data indices (or any other type of index with a defined retention
# period). It does this by comparing the creation date against today's date, and if the
# difference is more than the retention period in days, the index is deleted. You also
# have the option to define indexes that will not be included in the analysis.

import os
import json
import datetime
import boto3
import requests
from requests_aws4auth import AWS4Auth



# Get the endpoint from the env (set in the Lambda)
esEndPoint = os.environ["ES_ENDPOINT"]
# This string tells the ES api to get the indices and creation dates, return JSON, and sort by date
retrieveString = "/_cat/indices?h=index,creation.date.string&format=json&s=creation.date"
# Start a session
session = boto3.Session()
# Get the credentials from AWS
credentials = session.get_credentials()
# Get the region from the env
region = os.environ['AWS_REGION']
# Get the retention days from the env (set in the Lambda)
retention_days = int(os.environ['RETENTION_DAYS'])
# Get the indices to exclude from deletion
excluded_indices = os.environ['EXCLUDE_INDICES']
# Generate an auth header
auth = AWS4Auth(credentials.access_key, credentials.secret_key, region, 'es', session_token=credentials.token)

# Convert a datetime string into a datetime.

def convertDate(s):
    return datetime.datetime.strptime(s, '%Y-%m-%dT%H:%M:%S.%fZ')

# Gets the current indices and their creation dates

def retrieveIndicesAndDates():
    try:
        theResult = requests.get(esEndPoint+retrieveString,auth=auth)
    except Exception as e:
        print("Unable to retrieve list of indices with creation dates.")
        print(e)
        exit(3)
    return theResult.content

def lambda_handler(event, context):
    # Load the list of indices
    theIndices = json.loads(retrieveIndicesAndDates())
    # For date comparison
    today = datetime.datetime.now()
    print('Looking for records older than: ')
    print(today - datetime.timedelta(days=retention_days))
    # Walk through the list
    for entry in theIndices:
        #print(entry)
        # Ignore the index that has the Kibana config
        if excluded_indices.find(entry["index"]) > -1:
            continue
        # Compare the creation date with today
        diff = today - convertDate(entry["creation.date.string"])
        # If the index was created more than retention_days ago, blow it away
        if diff.days >= retention_days:
            print('About to delete ')
            print()
            print('Making api request to '+ esEndPoint+'/'+entry["index"])
            theresult = requests.delete(esEndPoint+'/'+entry["index"],auth=auth)
            theresult.raise_for_status()
    print('Execution done')
    return
