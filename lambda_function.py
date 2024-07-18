import json
import boto3
import datetime
import base64
import random
import string
import sys
import os
from boto3.dynamodb.conditions import Attr
sys.path.append(os.path.join(os.path.dirname(__file__), 'packages'))

from fastapi import FastAPI


dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('bef_Intent_Api_Dev')
current_time = datetime.datetime.now().isoformat()
app = FastAPI()


def lambda_handler(event, context):
    try:
        request = event['requestContext']['http']

        if request['method'] == 'POST':
            payload = decode_payload(event['body'])
            dynamodb_put_data(payload)     
            return {
                'statusCode': 200,
                'body': json.dumps('Item inserted/updated successfully in DynamoDB')
            }
        elif request['method'] == 'GET':
            # The intentid should be the last element in the path_parts list
            intentid_param = get_intent_id()
            print(f"intent id {intentid_param}")
            intentid_filter_expression = Attr('intentid').eq(intentid_param)
            items = dynamo_table_scan(intentid_filter_expression)
            return {
                'statusCode': 200,
                'body': json.dumps(items)
            }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }

def decode_payload(body):
    decoded_bytes = base64.b64decode(body)
    decoded_str = decoded_bytes.decode("ascii")
    return json.loads(decoded_str)

def dynamo_table_scan(filter_expression):
    get_response = table.scan(
                        FilterExpression=filter_expression
                    )
    items = get_response.get('Items', [])
    while 'LastEvaluatedKey' in get_response:
        paginate_response = table.scan(
            FilterExpression=filter_expression,
            ExclusiveStartKey=get_response['LastEvaluatedKey']
        )
        items.extend(paginate_response.get('Items', []))
    return items

def dynamodb_put_data(payload):
    intent_type_id = payload.get('intentTypeId')
    intent_key = payload.get('key', {})
    org_id, carrier_div, plan_id, state = (
        intent_key.get('orgId'),
        intent_key.get('carrierDiv'),
        intent_key.get('planId'),
        intent_key.get('state')
    )
    
    intentid = get_intentid(intent_type_id, org_id, carrier_div, plan_id, state)

    print(f"intent id : {intentid}")
   
    item = {
            'intentid': intentid, 
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
        existing_item = table.get_item(Key={'intentid': item['intentid']}).get('Item')
        print(f"existing data : {existing_item}")
        if existing_item:
            item['update_date'] = current_time
            item['create_date'] = existing_item['create_date']
        else:
            print(f"Item {intentid} not found in DynamoDB")

    except Exception as e:
        print(f"Error fetching item from DynamoDB: {str(e)}")

    put_response = table.put_item(Item=item)
    return put_response

def generate_random_intentid():
    intentid_length=24
    characters = string.ascii_lowercase + string.digits
    return ''.join(random.choice(characters) for _ in range(intentid_length))

def get_intentid(intent_type_id, org_id, carrier_div, plan_id, state):
    # filter_expression = Attr('intentTypeId').eq(intent_type_id)
    # filter_expression = (
    #     Attr('intentTypeId').eq(intent_type_id) &
    #     Attr('key.orgId').eq(org_id) &
    #     Attr('key.carrierDiv').eq(carrier_div) &
    #     Attr('key.planId').eq(plan_id) &
    #     Attr('key.state').eq(state)
    # )
    intent_id = generate_random_intentid()
    return intent_id

    # try:
    #     items = dynamo_table_scan(filter_expression)
    #     intent_id = generate_random_intentid()

    #     if items:
    #         for data in items:
    #             if data['intentTypeId'] == intent_type_id and data['key']['orgId'] == org_id and data['key']['carrierDiv'] == carrier_div and data['key']['planId'] == plan_id and data['key']['state'] == state:
    #                 intent_id = data['intentid']
    #     return intent_id
    # except Exception as e:
    #     return {
    #         'statusCode': 500,
    #         'body': json.dumps(f'failed to perform scan to get intent id: {str(e)}')
    #     }
    
@app.get("/evernorth/v1/intent/intent_lambda/{intentid}")
async def get_intent_id(intentid: str):
    return intentid