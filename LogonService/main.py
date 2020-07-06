from os import environ as osenv
from os import path
from datetime import datetime, timedelta
import json
import time
import schedule
import requests
from pymongo import MongoClient

# .ENV FILE FOR TESTING
#if path.exists('.env'):
#   from dotenv import load_dotenv
#   load_dotenv()

API_KEY = osenv.get('API_KEY','')
BF_USER = osenv.get('BF_USER','')
BF_PWD = osenv.get('BF_PWD','')
TOKEN_LIFE = int(osenv.get('TOKEN_LIFE', 8))
RUN_MINS = int(osenv.get('RUN_MINS', 1))
MONGO_HOST = osenv.get('MONGO_HOST','')
MONGO_PORT = int(osenv.get('MONGO_PORT',27017))
MONGO_DB = osenv.get('MONGO_DB','')
MONGO_USER = osenv.get('MONGO_USER','')
MONGO_PWD = osenv.get('MONGO_PWD','')

MONGO = MongoClient(MONGO_HOST, MONGO_PORT)
DB = MONGO[MONGO_DB]
DB.authenticate(MONGO_USER, MONGO_PWD)
COLLECTION = DB['sessions']

URI_ENDPOINT = 'https://identitysso.betfair.com/api/login'

def check_for_token():
    ''' Checks sessions Collection for record matching API key'''
    result = COLLECTION.find_one({"product": API_KEY})
    if result:
        return True, result
    return False, ''

def validate_token(token_record):
    ''' Validate if found token is valid '''
    issued = datetime.strptime(token_record['issued'], '%Y-%m-%d %H:%M:%S.%f')
    time_out = datetime.now() - timedelta(hours=TOKEN_LIFE)       
    if time_out < issued:
        print('VALIDTOKEN: ' + str(token_record['token']))
        print('ISSUED: ' + str(token_record['issued']))
        return True
    return False

def create_new_token(*args):
    ''' Validate if found token is valid '''
    now = datetime.now()
    headers = {'X-Application': f'{API_KEY}', 'Accept': 'application/json', 'Content-Type': 'application/x-www-form-urlencoded'}
    post_data = f'username={BF_USER}&password={BF_PWD}'
    response = requests.post(URI_ENDPOINT, headers=headers, data=post_data)
    token_data = response.json()
    if token_data['status'] == "SUCCESS":
        token_data.update({'issued' : str(now)})
        if len(args) > 0:
            result = COLLECTION.update_one({'_id': args[0]['_id']}, {'$set':token_data}, upsert=True)
        else:
            result = COLLECTION.insert_one(token_data)
        if result:
            return result
        else:
            print('TOKENERROR: Unable to get token')    

def do_it():
    token_exists, token_record = check_for_token()
    if token_exists == False:
        token_record = create_new_token()
    token_record = COLLECTION.find_one({"product": API_KEY})
    token_status = validate_token(token_record)
    if token_status == False:
        create_new_token(token_record)

def main():
    ''' Main entry point of the app '''
    do_it()
    schedule.every(RUN_MINS).minutes.do(do_it)

    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    ''' This is executed when run from the command line '''
    main()