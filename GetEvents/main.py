from os import environ as osenv
from os import path
from datetime import datetime, timedelta
import json
import time
import schedule
import requests
from pymongo import MongoClient

#.ENV FILE FOR TESTING
#if path.exists('.env'):
#   from dotenv import load_dotenv
#   load_dotenv()

API_KEY = osenv.get('API_KEY','')
RUN_MINS = int(osenv.get('RUN_MINS', 1))
MONGO_HOST = osenv.get('MONGO_HOST','')
MONGO_PORT = int(osenv.get('MONGO_PORT',27017))
MONGO_DB = osenv.get('MONGO_DB','')
MONGO_USER = osenv.get('MONGO_USER','')
MONGO_PWD = osenv.get('MONGO_PWD','')

MONGO = MongoClient(MONGO_HOST, MONGO_PORT)
DB = MONGO[MONGO_DB]
DB.authenticate(MONGO_USER, MONGO_PWD)
COLLECTION_EVENTS = DB['event_types']
COLLECTION_SESSIONS = DB['sessions']

URI_ENDPOINT = 'https://api.betfair.com/exchange/betting/rest/v1.0'

def get_active_session_token():
    ''' Checks sessions Collection for record matching API key'''
    result = COLLECTION_SESSIONS.find_one({"product": API_KEY})
    return result['token']

def get_live_data(*args):
    uri = f'{URI_ENDPOINT}/{args[0]}/'
    token = get_active_session_token()
    headers = {'X-Application': API_KEY,'X-Authentication': token, 
                'content-type': 'application/json'}
    post_data = json.dumps(args[1]) 
    response = requests.post(uri, headers=headers, data=post_data)
    return response.json()

def update_mongo(payload):
    events = payload
    for event in events:
        event = event['eventType']
        print('UPDATE: ' + str(event))
        COLLECTION_EVENTS.update_one({'name': event['name']}, {'$set': {'event_id' : event['id']}}, upsert=True)

def do_it():
    live_data = get_live_data('listEventTypes', {"filter" : {}})
    update_mongo(live_data)

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