import json
import boto3
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection
from botocore.exceptions import ClientError
import random

# connect to sqs
queue_url = 'https://sqs.us-east-1.amazonaws.com/558392889561/DiningSQS.fifo'
sqs = boto3.client('sqs',region_name='us-east-1')

# connect to opensearch
host = 'search-restaurants-eeg66qdw2cdq2ufjpullqfngg4.us-east-1.es.amazonaws.com'
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
    print('Start OpenSearch')
    businesses = os.search(index = 'restaurants-info', body={"size": 150, "query": {"match": {'categories.title':cuisine}}})
    print(businesses)
    return businesses['hits']['hits']


def business_details(ids, cuisine, people, date, time):
    print("Start DynamoDB")
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('yelp-restaurants')
    res = 'Hi! Thanks for your patience! Here are my {} suggestions for {} people, for {} at {}: \n '.format(cuisine, people, date, time)
    random_ids = random.sample(range(0, len(ids)), 5)
    order = 1
    for i in random_ids:
        try:
            response = table.get_item(Key={'business_id': ids[i]})
        except ClientError as e:
            print(e.response['Error']['Message'])
            continue
        
        item = response['Item']
        name = item['name']
        address = item['address']
        # rating = str(item['rating'])
        # zipcode = item['zip_code']
        res += "{}. {}, located at {}.\n Enjoy your meal! \n".format(order, name, address)
        order += 1
        
    return res


def send_sns(phone, message):
    print("Start sending message to {}".format(phone))
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
        MessageAttributes=attributes,
        MessageDeduplicationId='chatbotfall2021_cw3326',
        MessageGroupId='123456789987654321'
    )

    print(response)



def lambda_handler(event, context):
    response = sqs.receive_message(
        QueueUrl=queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=60,
        WaitTimeSeconds=0
    )
    
    try:
        message = response['Messages'][0]
        receipt_handle = message['ReceiptHandle']
        attributes = message['MessageAttributes']
        cuisine = attributes['cuisine']['StringValue']
        phone = attributes['phone']['StringValue']
        people = attributes['people']['StringValue']
        date = attributes['date']['StringValue']
        time = attributes['time']['StringValue']
        
        ids = opensearch(cuisine)
        ids = list(map(lambda x: x['_id'], ids))
        print("Matched Results:", len(ids))
        
        details = business_details(ids, cuisine, people, date, time)
        send_sns("+1" + phone, details)
        
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        
    except Exception as e:
        print(e)
