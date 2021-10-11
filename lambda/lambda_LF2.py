import json
import boto3
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection
from random import shuffle

# connect to sqs
queue_url = 'https://sqs.us-east-1.amazonaws.com/964889031791/user_preference.fifo'
sqs = boto3.client('sqs',region_name='us-east-1')

# connect to opensearch
host = 'search-restaurants-yelp-i3ylr4bj5k5bcwzutigrub775e.us-east-1.es.amazonaws.com'
region = 'us-east-1'

service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

os = OpenSearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)

def opensearch(cuisine):
    print('enter opensearch')
    businesses = os.search(size = 100, index = 'restaurants-yelp', body={"query": {"match": {'categories.title':cuisine}}})
    print(businesses['hits']['hits'])
    return businesses['hits']['hits']


def business_details(ids, cuisine, people, date, time):
    print('enter dynamodb')
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
        print(response)
        item = response['Item']
        name = item['name']
        address = ' '.join(item['address'])
        rating = str(item['rating'])
        zipcode = item['zip_code']
        res += "{}. {}, located at {}.\n".format(i, name, address)
        i += 1
    res += 'Enjoy your meal! '
    print(res)
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
        print(cuisine,phone,people,date,time)
        ids = opensearch(cuisine)
        ids = list(map(lambda x: x['_id'], ids))
        shuffle(ids)
        print(len(ids))
        details = business_details(ids, cuisine, people, date, time)
        send_sns("+1" + phone, details)
        sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle
            )
        print('delete successfully')
    except Exception as e:
        print(e)