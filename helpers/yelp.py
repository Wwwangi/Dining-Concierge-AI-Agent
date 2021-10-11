import json
import boto3
import requests
import datetime
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection

# basic request settings
yelp_search_api = "https://api.yelp.com/v3/businesses/search"
payload = ""
headers = {
    "Authorization": "Bearer B9QaCTN70BoZRB_rQwteEU30as8iHmd4i646Fsr4kjZSaJP0REexqORtOt94PDMVjxK0TgEMY7oRbtCysSOMdQhZm2EfI5QIapuwOWy8zZYRBd4GDKt6OvgVgkxfYXYx",
    'cache-control': "no-cache"
    }
# Number of business results to return
limit = 50
# to get the next page of results
offset = 5


# Get businesses information from yelp api
businesses = []
cuisines = ['chinese','indian','thai','american','mexican','brunch','japanese','italian']
for cuisine in cuisines:
    print(cuisine)
    for count in range(offset):
        print(count)
        print(len(businesses))
        requestData = {
            "term": cuisine + " restaurants",
            "location": "manhattan",
            "limit": limit,
            "offset": 50*count
            }
        response = requests.get(yelp_search_api, data=payload, headers=headers, params=requestData)
        message = json.loads(response.text)
        try:
            business = message['businesses']
            businesses = businesses + business
        except:
            pass

# store the businesses information into dynamo db
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('yelp-restaurants')
for business in businesses:
    tableEntry = {
        'id': business['id'],
        'name': business['name'],
        'is_closed': business['is_closed'],
        'categories': business['categories'],
        'rating': int(business['rating']),
        'review_count': int(business['review_count']),
        'address': business['location']['display_address']
    }
    if (business['coordinates'] and business['coordinates']['latitude'] and business['coordinates']['longitude']):
        business['latitude'] = str(business['coordinates']['latitude'])
        business['longitude'] = str(business['coordinates']['longitude'])
    if (business['location']['zip_code']):
        business['zip_code'] = business['location']['zip_code']
    table.put_item(
        Item={
            'insertedAtTimestamp': str(datetime.datetime.now()),
            'id': tableEntry['id'],
            'name': tableEntry['name'],
            'is_closed': tableEntry['is_closed'],
            'categories': tableEntry['categories'],
            'address': tableEntry['address'],
            'latitude': tableEntry.get('latitude', None),
            'longitude': tableEntry.get('longitude', None),
            'review_count': tableEntry['review_count'],
            'rating': tableEntry['rating'],
            'zip_code': tableEntry.get('zip_code', None)
            }
        )


# Connect to the OpenSearch Service
host = 'search-restaurants-yelp-i3ylr4bj5k5bcwzutigrub775e.us-east-1.es.amazonaws.com'
region = 'us-east-1'

service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

search = OpenSearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = awsauth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection
)

count = 1
for business in businesses:
    count+=1
    if count%100==0:
        print(count)
        print(business)
    document = {
        'id': business['id'],
        'categories': business['categories']
    }
    search.index(index="restaurants-yelp", doc_type="restaurants-yelp", id=business['id'], body=document)


