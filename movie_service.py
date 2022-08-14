from urllib import response
from boto3 import resource
from boto3.dynamodb.conditions import Attr, Key
import config
import hashlib

AWS_ACCESS_KEY_ID = config.AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY = config.AWS_SECRET_ACCESS_KEY
REGION_NAME = config.REGION_NAME
ENDPOINT_URL = config.ENDPOINT_URL

resource = resource(
    'dynamodb',
    endpoint_url=ENDPOINT_URL,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=REGION_NAME
)


def create_table_movie():
    table = resource.create_table(
        TableName='Movie',
        KeySchema=[
            {
                'AttributeName': 'imdb_title_id',
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'imdb_title_id',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    table2 = resource.create_table(
        TableName='User',
        KeySchema=[
            {
                'AttributeName': 'username',
                'KeyType': 'HASH'
            }
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'username',
                'AttributeType': 'S'
            }
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
    return table, table2


MovieTable = resource.Table('Movie')
UserTable = resource.Table('User')


def write_to_movie(item):
    response = MovieTable.put_item(
        Item=item
    )
    return response


def create_user(username, password):
    response = UserTable.put_item(
        Item={
            'username': username,
            'password': password
        }
    )
    return response


def get_user(username):
    response = UserTable.get_item(
        Key={
            'username': username
        }
    )
    return response


def title_by_director_range(director, start, end):
    response = MovieTable.scan(
        FilterExpression=Attr('director').eq(
            director) & Attr('year').between(start, end)
    )
    return response


def get_user_reviews(review, language):
    response = MovieTable.scan(
        FilterExpression = Attr('language').eq(language) & Attr('reviews_from_users').gt(review)
    )
    def comp(e):
        return e['reviews_from_users']
    response['Items'].sort(reverse=True, key=comp)
    return response

def get_title(year, country):
    response = MovieTable.scan(
        FilterExpression = Attr('country').eq(country) & Attr('year').eq(year)
    )
    print("Gooooooooooooooooooooooooooooooooood")
    print("Hello",response['Items'])
    def comp(e):
        return e['budget']
    response['Items'].sort(reverse=True,key=comp)
    return response