import json
import boto3
from requests_aws4auth import AWS4Auth
import requests

# connect to sqs
queue_url = 'https://sqs.us-east-1.amazonaws.com/964889031791/user_preference.fifo'
sqs = boto3.client('sqs',region_name='us-east-1')

# connect to opensearch
host = 'search-restaurants-info-f67f3abjypdkhfywfpqjxle2ya.us-east-1.es.amazonaws.com'
region = 'us-east-1'
index = 'restaurants-info'
url = host + index
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

def opensearch(cuisine):
    print('enter opensearch')
    businesses = requests.put(url, auth=awsauth, json={"query": {"match": {'categories.title':cuisine}}})
    print(businesses)
    return businesses['hits']['hits']


def business_details(ids, cuisine, people, date, time):
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('yelp-restaurants')
    res = 'Hi! Thanks for your patience! Here are my {} suggestions for {} people, for {} at {}: \n '.format(cuisine, people, date, time)
    i = 0
    for id in ids:
        if i==5:
            break
        response = table.get_item(
            Key={
                'id': id
            }
        )
        item = response['Item']
        name = item['name']
        address = item['address']
        rating = str(item['rating'])
        zipcode = item['zip_code']
        res += "{}. {}, located at {}.\n Enjoy your meal! \n".format(i, name, address)
        i += 1
    return res


def send_sns(phone, message):
    sns = boto3.client('sns',region_name='us-east-1')
    attributes = {
        'AWS.SNS.SMS.SenderID': {
            'DataType': 'String',
            'StringValue': 'TestSender'
        },
        'AWS.SNS.SMS.SMSType': {
            'DataType': 'String',
            'StringValue': 'Transactional'
        }
    }
    response = sns.publish(
        PhoneNumber=phone,
        Message=message,
        MessageAttributes=attributes
    )
    print(phone)
    print(response)


# poll messages from sqs, search for business ids using opensearch, get details from dynamo db, send sns message to users
def lambda_handler(event, context):
    messages = sqs.receive_message(QueueUrl=queue_url, MessageAttributeNames=['All'])
    print(messages)
    try:
        message = messages['Messages'][0]
        receipt_handle = message['ReceiptHandle']
        attributes = message['MessageAttributes']
        cuisine = attributes['cuisine']['StringValue']
        phone = attributes['phone']['StringValue']
        people = attributes['people']['StringValue']
        date = attributes['date']['StringValue']
        time = attributes['time']['StringValue']
        ids = opensearch(cuisine)
        ids = list(map(lambda x: x['_id'], ids))
        print(ids)
        details = business_details(ids, cuisine, people, date, time)
        send_sns("+1" + phone, details)
        sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
    except Exception as e:
        print(e)