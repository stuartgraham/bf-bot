from os import environ as osenv
from os import path
from datetime import datetime, timedelta
import json
import time
import schedule
import requests
from pymongo import MongoClient

#.ENV FILE FOR TESTING
if path.exists('.env'):
   from dotenv import load_dotenv
   load_dotenv()

API_KEY = osenv.get('API_KEY','')
RUN_MINS = int(osenv.get('RUN_MINS', 1))
MONGO_HOST = osenv.get('MONGO_HOST','')
MONGO_PORT = int(osenv.get('MONGO_PORT',27017))
MONGO_DB = osenv.get('MONGO_DB','')
MONGO_USER = osenv.get('MONGO_USER','')
MONGO_PWD = osenv.get('MONGO_PWD','')
INPLAY = bool(osenv.get('INPLAY',''))
SPORTS = osenv.get('SPORTS','')


MONGO = MongoClient(MONGO_HOST, MONGO_PORT)
DB = MONGO[MONGO_DB]
DB.authenticate(MONGO_USER, MONGO_PWD)
COLLECTION_EVENTS = DB['event_types']
COLLECTION_COMPS = DB['competitions']
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
    response = response.json()
    return response

def update_mongo(payload):
    ''' Will interate payload and upsert to mongo'''
    comps = payload
    for comp in comps:
        print(f'UPDATE: {str(comp)}')
        COLLECTION_COMPS.update_one({'name': comp['competition']['name']}, {'$set': {'event_id' : comp['competition']['id'], 'inplay' : False, 'region' : comp['competitionRegion']}}, upsert=True)

def reset_inplay_state():
    ''' Updates any remaining markets to inplay false'''
    COLLECTION_COMPS.update_many({'inplay' : True}, { '$set':{'inplay' : False}})
    result_count = 0
    for result in COLLECTION_COMPS.find({'inplay' : True}):
        result_count += 1
        print(f'UPDATE: {str(result)}')
    if result_count == 0:
        print('NOUPDATE: No Inplay resets required')

def update_inplay_state(payload):
    ''' Will interate payload and upsert to mongo'''
    comps = payload
    print(f'INPLAY: {len(comps)} competitions currently In-Play')
    for comp in comps:
        print('UPDATE: ' + str(comp))
        COLLECTION_COMPS.update_one({'event_id': comp['competition']['id']}, {'$set': {'inplay' : True}}, upsert=True)
    print("LISTING ALL IN-PLAY COMPS")
    for result in COLLECTION_COMPS.find({'inplay': True}):
        print(result)

def lookup_sports():
    sports_ids = []
    sport_names = SPORTS.split(',')
    for sport_name in sport_names:
        result = COLLECTION_EVENTS.find_one({'name' :{'$regex' : f'^{sport_name}$', '$options' : 'i'}})
        sports_ids.append(int(result['event_id']))
    return sports_ids

def do_it():
    sports_id = lookup_sports()
    live_data = get_live_data('listCompetitions', {"filter" : {"eventTypeIds": sports_id}})
    update_mongo(live_data)
    reset_inplay_state()
    inplay_data = get_live_data('listCompetitions', {"filter" : {"eventTypeIds": sports_id, "inPlayOnly" : INPLAY}})
    update_inplay_state(inplay_data)

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