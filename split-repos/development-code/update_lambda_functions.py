import boto3
import json


# Load configuration from JSON file
with open('lambda_configuration.json', 'r') as config_file:
    functions = json.load(config_file)
    
aws_region = functions[0].get('region', '')


# Initialize the S3 and Lambda clients
s3_client = boto3.client('s3', region_name=aws_region)
lambda_client = boto3.client('lambda', region_name=aws_region)

# Process each Lambda function configuration
for function in functions:
    function_name = function['function_name']
    s3_key = function['s3_key']
    s3_bucket = function['s3_bucket']
    
    # Update Lambda function code
    try:
        response = lambda_client.update_function_code(
            FunctionName=function_name,
            S3Bucket=s3_bucket,
            S3Key=s3_key
        )
        print(f'Updated {function_name}: {response}')
    except Exception as e:
        raise f"update {function_name} failed with error {e}"
