import json
import boto3
import datetime
import base64
from boto3.dynamodb.conditions import Attr


dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('IntentsTable')
current_time = datetime.datetime.now().isoformat()

def lambda_handler(event, context):
    try:
        decodedBytes = base64.b64decode(event['body'])
        decodedStr = decodedBytes.decode("ascii")
        payload = json.loads(decodedStr)
        print(f"query string parameters {event['queryStringParameters']}")
        
        request_method = event['requestContext']['http']['method']

        if request_method == 'POST':
            dynamodb_put_data(payload)     
            return {
                'statusCode': 200,
                'body': json.dumps('Item inserted/updated successfully in DynamoDB')
            }
        elif request_method == 'GET':
            intentid = event['queryStringParameters']['intentid']
            items = dynamo_table_scan(intentid)
            return {
                'statusCode': 200,
                'body': json.dumps(items)
            }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }


def dynamo_table_scan(intentid):
    filter_expression = Attr('intentTypeId').eq(intentid)
    get_response = table.scan(
                        FilterExpression=filter_expression
                    )
    items = get_response.get('Items', [])
    while 'LastEvaluatedKey' in response:
        response = table.scan(
            FilterExpression=filter_expression,
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items.extend(response['Items'])
    return items

def dynamodb_put_data(payload):
    intent_type_id = payload.get('intentTypeId')
    intent_key = payload.get('key', {})
    org_id = intent_key.get('orgId')
    carrier_div = intent_key.get('carrierDiv')
    plan_id = intent_key.get('planId')
    state = intent_key.get('state')
    item = {
            '_id': f"{intent_type_id}-{org_id}", 
            'intentTypeId': intent_type_id,
            'key': {
                'orgId': org_id,
                'carrierDiv': carrier_div,
                'planId': plan_id,
                'state': state
            },
            'create_date': current_time,
            'update_date': None
        }
    try:
        item_key = {'_id': item['_id']}
        existing_item = table.get_item(Key=item_key).get('Item')
        print(f"existing data : {existing_item}")
        if existing_item:
            item['update_date'] = current_time
            item['create_date'] = existing_item['create_date']
        else:
            print(f"Item {item_key} not found in DynamoDB")

    except Exception as e:
        print(f"Error fetching item from DynamoDB: {str(e)}")
    put_response = table.put_item(Item=item)
    return put_response