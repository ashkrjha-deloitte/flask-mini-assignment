from telnetlib import AUTHENTICATION
from urllib import response
from flask import Flask, request, jsonify, make_response, g
from functools import wraps
import jwt
import movie_service as dynamodb
from werkzeug.security import generate_password_hash, check_password_hash
import re
import datetime
from collections import OrderedDict

app = Flask(__name__)
app.config['SECRET_KEY'] = 'thisissecret'

AUTHENTICATION_FAILED = "Failed to authenticate request"
INVALID_TOKEN = 'Invalid token'


def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if 'token' in kwargs:
            try:
                jwt.decode(
                    kwargs['token'], app.config['SECRET_KEY'], algorithms=['HS256'])
                return f(*args, **kwargs)
            except Exception:
                return jsonify({'message': AUTHENTICATION_FAILED, 'error_code': INVALID_TOKEN}), 300
        elif 'token' in request.cookies:
            try:
                jwt.decode(request.cookies.get('token'),
                           app.config['SECRET_KEY'], algorithms=['HS256'])
                return f(*args, **kwargs)
            except Exception:
                return jsonify({'message': AUTHENTICATION_FAILED, 'error_code': INVALID_TOKEN}), 300
        else:
            return jsonify({'message': AUTHENTICATION_FAILED, 'error_code': INVALID_TOKEN}), 300
    return decorated


@app.route('/createTable')
def root_route():
    dynamodb.create_table_movie()
    return 'Table created'


def load_csv():
    results = []
    count = 0
    rexp = ',(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))'
    with open('movies.csv', 'r') as f:
        lines = [line.rstrip() for line in f]
        for line in lines:
            words = re.split(rexp, line)
            if count != 0:
                if words[20]:
                    user_review = int(words[20])
                else:
                    user_review = 0
                results.append({'imdb_title_id': words[0], 'title': words[1].strip("\\\""), 'original_title': words[2].strip("\\\""), 'year': int(words[3]), 'date_published': words[4].strip("\\\""), 'genre': words[5].strip("\\\""), 'duration': int(words[6]), 'country': words[7].strip("\\\""), 'language': words[8].strip("\\\""), 'director': words[9].strip("\\\""), 'writer': words[10].strip("\\\""), 'production_company': words[11].strip("\\\""),
                               'actors': words[12].strip("\\\""), 'description': words[13].strip("\\\""), 'avg_vote': words[14], 'votes': words[15], 'budget': words[16].strip("\\\""), 'usa_gross_income': words[17].strip("\\\""), 'worldwide_gross_income': words[18].strip("\\\"$"), 'metascore': words[19], 'reviews_from_users': user_review, 'reviews_from_critics': words[21].rstrip("\\n")})
            count += 1
    return results


@app.route('/loadTable', methods=['POST'])
def add_movies():
    results = load_csv()
    for line in results:
        response = dynamodb.write_to_movie(line)
    return response


@app.route('/register', methods=['POST'])
def register_user():
    data = request.get_json()
    hashed_password = generate_password_hash(data['password'], method='sha256')
    dynamodb.create_user(username=data['username'], password=hashed_password)
    response = jsonify({'message': 'New user created!'})
    return response


@app.route('/movies/<string:director>/<int:start>/<int:end>', methods=['GET'])
def get_book(director, start, end):
    response = dynamodb.title_by_director_range(director, start, end)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        if ('Items' in response):
            return {'Title': response['Items'][0]['title']}
        return {'msg': 'Item not found!'}
    return {
        'msg': 'Some error occured',
        'response': response
    }


@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data['username']
    query = dynamodb.get_user(username=username)
    if query["Item"]:
        token = jwt.encode({'username': username,
                            'exp': datetime.datetime.utcnow() + datetime.timedelta(minutes=15)},
                           app.config['SECRET_KEY'])
        return jsonify({'token': token.decode('utf-8'), 'message': 'token generated'}), 200
    return jsonify({'message': AUTHENTICATION_FAILED, 'error_code': 'Failed Auth'}), 200


@app.route('/movies/<int:review>/<string:language>')
def get_reviews(review, language):
    response = dynamodb.get_user_reviews(review, language)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        if ('Items' in response):
            return { 'Item': response['Items'] }
        return {'msg': 'Item not found'}
    return {  
        'msg': 'Some error occcured',
        'response': response
    } 

@app.route('/movies/<int:year>/<string:country>', methods=['GET'])
def generate_highest_budget_title(year, country):
    response = dynamodb.get_title(year,country)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        if ('Items' in response):
            return { 'Item': response['Items'][0] }
        return {'msg': 'Item not found'}
    return {  
        'msg': 'Some error occcured',
        'response': response
    } 

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5001, debug=True)
