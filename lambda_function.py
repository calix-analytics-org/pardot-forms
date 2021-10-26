import requests
import json
import boto3
from botocore.exceptions import ClientError #https://stackoverflow.com/questions/42975609/how-to-capture-botocores-nosuchkey-exception
import xmltodict
import pandas as pd
import datetime
from io import StringIO
from string import digits
from pardot_api import Pardot

def format_datetime_for_filename(datetime_str : str):
    '''Strip special chars and slice first 10 digits'''
    return ''.join(c for c in datetime_str if c.isdigit())[:10]

def get_last_runtime(table):
    '''Retrieve last runtime from dynamodb'''

    last_runtime = None

    try:
        last_runtime = table.get_item(Key = {'state':'pardot_forms_runtime'})['Item']
        last_runtime = last_runtime['value']
    except:
        pass

    return last_runtime

def persist_last_runtime(table, last_runtime):
    '''Save last runtime in dynamodb'''

    table.put_item(
        Item = {'state':'pardot_forms_runtime',
                'value':last_runtime})

    return

def copy_df_to_s3(client, df, bucket, filepath):
    ''' Uploads dataframe to s3 bucket, lifted from
    https://stackoverflow.com/questions/38154040/save-dataframe-to-csv-directly-to-s3-python'''
    
    csv_buf = StringIO()
    df.to_csv(csv_buf, index=False)
    csv_buf.seek(0)
    client.put_object(Bucket=bucket, Body=csv_buf.getvalue(), Key=filepath)

def get_forms_list(full_refresh = False):
    
    # Set some constanct

    state_bucket = 'calix-ads-lambda-states'
    state_key = 'pardot-forms/state.json'
    data_bucket = 'calix-ads-data-stage'

    # Initialize boto clients
    
    secrets_client = boto3.client(
        'secretsmanager',
        region_name='us-west-2'
    )

    ddb_client = boto3.resource('dynamodb',
                    region_name = 'us-west-2')
    state_table = ddb_client.Table('lambda_states')

    s3_client = boto3.client(
        's3',
        region_name='us-west-2'
    )

    credentials = json.loads(secrets_client.get_secret_value(SecretId='credentials/pardot')['SecretString'])
    pardot = Pardot(credentials)

    # Set date after which we pull changes to forms

    if full_refresh:
        last_updated = datetime.datetime.min.strftime('%Y-%m-%d %H:%M:%S')
    else:
        
        # Try to load state from file. If file doesn't exist, set last_updated to min date similar to in a full_refresh
        
        last_updated = get_last_runtime(state_table)
        if not last_updated:
            last_updated = datetime.datetime.min.strftime('%Y-%m-%d %H:%M:%S')

    # Paginate through forms that have been updated since last_updated and save files to s3
    while True:
        resp = pardot.get_forms(query = f'sort_by=updated_at&sort_order=ascending&updated_after={last_updated}')
        resp_parsed = xmltodict.parse(resp.content)

        # Check for errors in response
        if 'err' in resp_parsed['rsp'].keys():
            raise Exception(resp_parsed['rsp']['err']['#text'])

        if int(resp_parsed['rsp']['result']['total_results']) == 0:
            print('No results left to process. Done.')
            break
        
        data = resp_parsed['rsp']['result']['form']
        if int(resp_parsed['rsp']['result']['total_results']) == 1:
            # If we are only loading 1 record from a dict, we need to account
            # for pandas behavior as mentioned here: 
            # https://stackoverflow.com/questions/69727128/pandas-incorrectly-reading-nested-ordereddict-with-len-1/69727295#69727295
            data = [data]

        df = pd.DataFrame(data)

        # Convert ordered dict in campaign field to regular dict
        df.campaign = df.campaign.apply(lambda x: dict(x))
        
        print(f'{last_updated} : {df.shape}')
        formatted_date = format_datetime_for_filename(last_updated)
        copy_df_to_s3(s3_client, df, data_bucket, f'pardot/forms/forms_{formatted_date}.csv')
        print(df.updated_at.head())
        last_updated = df.updated_at.max()
        print(last_updated)

    # Persist state
    persist_last_runtime(state_table, last_updated)

def lambda_handler(event, context):
    get_forms_list(full_refresh = False)

# Uncomment for local dev
#lambda_handler(None, None)