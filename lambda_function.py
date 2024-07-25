import json
import boto3
import datetime
import base64
import random
import string
from boto3.dynamodb.conditions import Attr
from fastapi import FastAPI
from mangum import Mangum


app = FastAPI()
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('bef_Intent_Api_Dev')
current_time = datetime.datetime.now().isoformat()
handler = Mangum(app)

@app.get("/evernorth/v1/intent/intent_lambda/{intentid}")
async def get_intent_data(intentid: str):
    try:
        print(f"Intent id : {intentid}")
        intentid_filter_expression = Attr('intentid').eq(intentid)
        items = dynamo_table_scan(intentid_filter_expression)
        print(f"response items : {items}")
        if not items:
            return {
                'statusCode': 404,
                'body': json.dumps(f'Item not found')
            }
        
        return {
            'statusCode': 200,
            'body': json.dumps(items)
        }
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }
    
@app.post("/evernorth/v1/intent/intent_lambda")
async def post_intent_data(data):
    try:
        print(f"post event data : {data}")
        payload = decode_payload(data)
        dynamodb_put_data(payload)
        return {
            'statusCode': 200,
            'body': json.dumps('Item inserted/updated successfully in DynamoDB')
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
    intentid = generate_random_intentid()
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
        if existing_item:
            item['update_date'] = current_time
            item['create_date'] = existing_item['create_date']
        table.put_item(Item=item)
    except Exception as e:
        print(f"Error fetching item from DynamoDB: {str(e)}")

def generate_random_intentid():
    intentid_length = 24
    characters = string.ascii_lowercase + string.digits
    return ''.join(random.choice(characters) for _ in range(intentid_length))

