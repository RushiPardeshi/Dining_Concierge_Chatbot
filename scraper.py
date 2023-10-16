!pip install requests_aws4auth

import boto3
import datetime
import requests
import json, os
from datetime import datetime
# from elasticsearch import Elasticsearch
from botocore.exceptions import ClientError
from decimal import *
from time import sleep

# from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

aws_access_key_id       = ""
aws_secret_access_key   = ""
region                  = ""
service                 = ""
# host                    = ""

dynamodb = boto3.resource (
                            service_name            = "dynamodb",
                            aws_access_key_id       = aws_access_key_id,
                            aws_secret_access_key   = aws_secret_access_key,
                            region_name             = "us-west-2",
                        )

table = dynamodb.Table('Yelp-restaurant')
auth = AWS4Auth(aws_access_key_id, aws_secret_access_key, region, service)

import datetime

# dynamodb = boto3.resource('dynamodb', region_name='us-west-2')

# table = dynamodb.Table('Yelp-restaurants')
cuisine_types = ['chinese', 'indian', 'mexican']

def save_data_to_json(index_data, restaurant_data):
    with open('restaurant_index.json', 'w') as index_file:
        json.dump(index_data, index_file, indent=4)

    with open('restaurant_data.json', 'w') as data_file:
        json.dump(restaurant_data, data_file, indent=4)
        
for cuisine_type in cuisine_types:
    offset = 0
    for i in range(0, 1):
        offset += 50
        PARAMETERS = {
            'term': 'restaurant',
            'location': 'New York',
            'radius': 40000,
            'categories': cuisine_type,
            'limit': 50,
            'offset': offset,
            'sort_by': 'best_match'
        }
        response = requests.get(url=ENDPOINT, params=PARAMETERS, headers=HEADERS)
        # response = http.request('GET',
        #                 url=ENDPOINT,
        #                 body = json.dumps(some_data_structure),
        #                 headers =HEADERS,
        #                 retries = False)
        if response.status_code == 200:
            business_data = response.json()
            with open('restaurant_data.json', 'w') as datafile:
              json.dump(business_data, datafile, indent=4)
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")


with table.batch_writer() as batch:
    for biz in business_data['businesses']:
                try:
                    batch.put_item(
                        Item={
                            'businessId': biz['id'],
                            'name': biz['name'],
                            'category': biz['categories'][0]['alias'],
                            'address': biz['location']['address1'],
                            'city': biz['location']['city'],
                            'zipcode': biz['location']['zip_code'],
                            'latitude': Decimal(str(biz['coordinates']['latitude'])),
                            'longitude': Decimal(str(biz['coordinates']['longitude'])),
                            'reviewCount': biz['review_count'],
                            'rating': Decimal(str(biz['rating'])),
                            'phone': biz['phone'],
                            'url': str(biz['url']),
                            'insertedAtTimestamp': str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                        },
                    )

                except ClientError as e:
                    print(e.response['Error']['Code'])


