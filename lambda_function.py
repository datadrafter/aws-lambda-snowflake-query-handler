from snowflake_query_handler import snowflake_query_handler

import json
import boto3

secrets_client = boto3.client(
    'secretsmanager',
    region = '<your-region>'
)

# Get credentials from AWS Secret Manager 
credentials = json.loads(secrets_client.get_secret_value(SecretId='credentials/snowflake')['SecretString'])

# SNOWFLAKE
SNOWFLAKE_ACCOUNT = credentials['SNOWFLAKE_ACCOUNT']
SNOWFLAKE_WAREHOUSE = credentials['SNOWFLAKE_WAREHOUSE']
SNOWFLAKE_USER = credentials['SNOWFLAKE_USER']
SNOWFLAKE_USER_ROLE = credentials['SNOWFLAKE_USER_ROLE']
SNOWFLAKE_USER_KEY = credentials['SNOWFLAKE_USER_KEY']
SNOWFLAKE_USER_KEY_PHRASE = credentials['SNOWFLAKE_USER_KEY_PHRASE']

def get_query_results(query):
    
    # Init Snowflake Class
    snowflake = snowflake_query_handler(SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_USER, SNOWFLAKE_USER_ROLE, SNOWFLAKE_USER_KEY, SNOWFLAKE_USER_KEY_PHRASE)
    
    # Execute query and returns the result
    result = snowflake.execute_query(query)
    
    # Parses result to json
    json_result = json.dumps(result, default=str, ensure_ascii=False)

    return json_result

def lambda_handler(request, context):
    
    response = {}
    
    if request.get('query'):
        response = get_query_results(request['query'])
        
    return response