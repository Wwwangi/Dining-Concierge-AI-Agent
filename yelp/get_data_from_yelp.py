import requests
import json
import boto3
import datetime
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection


# yelp api config
yelp_api = "https://api.yelp.com/v3/businesses/search"
headers = {
    'Authorization': 'Bearer RRMn7va_S_AVvA2n6PesOluqPlzB5TL6AO7r-1YINczA3QB9LJv-shZFOsQeuXx1SjPLIoPhtrIm69GR8lENI9LPOhbixrVeN_RGltY5oUchL-WLjKSrcnTi1cZgYXYx',
    'cache-control': "no-cache"
}

# each cuisine has the search limitation up to 200 (limit * offsets)
limit = 50
offset = 4
restaurants = []

# get 7 kind of cuisines from yelp api
# and store to "restaurants" list
cuisines = ['caribbean', 'japanese', 'italian', 'chinese', 'american', 'mexico', 'korean']
for cuisine in cuisines:
    print(cuisine)
    for i in range(offset):
        print("No.{} - {}:".format(i*limit+1, i*limit+limit))
        requestData = {
            "term": "restaurant",
            "categories": cuisine,
            "location": "NYC",
            "limit": limit,
            "offset": limit * i
        }
        response = requests.get(yelp_api, headers=headers, params=requestData)
        message = json.loads(response.text)
        try:
            restaurants += message["businesses"]
        except:
            pass

# store restaurant info to DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('yelp-restaurants')
for restaurant in restaurants:
    table.put_item(
        Item={
            "business_id": restaurant["id"],
            "name": restaurant["name"],
            "address": restaurant["location"]["display_address"],
            "review_count": int(restaurant["review_count"]),
            "rating": str(restaurant["rating"]),
            "zip_code": restaurant["location"]["zip_code"],
            "insertedAtTimestamp": str(datetime.datetime.now())
        }
    )

# store restaurant info to OpenSearch
host = 'search-restaurants-eeg66qdw2cdq2ufjpullqfngg4.us-east-1.es.amazonaws.com'
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

for restaurant in restaurants:
    document = {
        'id': restaurant['id'],
        'categories': restaurant['categories']
    }
    search.index(index="restaurants-info", doc_type="restaurant-info", id=restaurant['id'], body=document)
