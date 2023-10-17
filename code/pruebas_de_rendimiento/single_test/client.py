import json
import time
import traceback
import uuid
from datetime import datetime
from threading import Thread

import requests
from requests.adapters import HTTPAdapter, Retry

# CONSTANTS:
OBSERVATIONS_GET_ENDPOINT = 'http://data.mobility.brussels/traffic/api/counts/'
POST_OBSERVATION_ENDPOINT = 'http://localhost:14080/api/addobservation'

FREQUENCY_IN_MINUTES = 1
OUTPUT_FORMAT = 'json'
REQUEST_PARAM = 'live'
INCLUDE_LANES = True
SINGLE_VALUE = False

OBSERVATION_GET_REQUEST_PARAMS = {
    'request': REQUEST_PARAM,
    'outputFormat': OUTPUT_FORMAT,
    'includeLanes': INCLUDE_LANES,
    'singleValue': SINGLE_VALUE,
    'interval': FREQUENCY_IN_MINUTES
}

SLEEP_TIME = FREQUENCY_IN_MINUTES * 60


TOPIC_ID = ''


def make_observation_get_request(max_num_requests=50):
    try:
        session = requests.Session()

        retries = Retry(total=max_num_requests,
                        backoff_factor=0.1,
                        status_forcelist=[500, 502, 503, 504])

        session.mount('http://', HTTPAdapter(max_retries=retries))

        response = session.get(
            url=OBSERVATIONS_GET_ENDPOINT,
            params=OBSERVATION_GET_REQUEST_PARAMS,
            verify=True)
        return response.json()
    except Exception as e:
        print('Failed to fetch observations!!!')
        traceback.print_exc()


def process_and_publish_observations(json_object):
    data = None

    # print(json_object)

    try:
        data = json_object['data']
    except:
        print(json_object)

    cant_of_observations = 0
    for sensor in data:
        obs = data[sensor]['results']['1m']['t1']


        if obs['speed'] is not None and obs['end_time'] is not None:
            speed_observation = json.dumps({
                'id': str(uuid.uuid4()),
                'streamId': sensor + '_SPEED-speed',
                'data': {'value': obs['speed']},
                'resultTime': obs['end_time'],
                'topic': TOPIC_ID
            })
            cant_of_observations += 1

            response_string = requests.put(POST_OBSERVATION_ENDPOINT, speed_observation).content.decode()
            response_json = json.loads(response_string)

            if response_json['status'] == 'FAIL':
                print("ERROR POSTING SPEED OBSERVATION: " + response_json['cause'])

        if obs['count'] is not None and obs['end_time'] is not None:
            count_observation = json.dumps({
                'id': str(uuid.uuid4()),
                'streamId': sensor + '_COUNT-count',
                'data': {'value': obs['count']},
                'resultTime': obs['end_time'],
                'topic': TOPIC_ID
            })
            cant_of_observations += 1

            response_string = requests.put(POST_OBSERVATION_ENDPOINT, count_observation).content.decode()
            response_json = json.loads(response_string)

            if response_json['status'] == 'FAIL':
                print("ERROR POSTING COUNT OBSERVATION: " + response_json['cause'])

    return cant_of_observations


def get_time_string():
    return datetime.now().strftime("%Y/%m/%d %H:%M")


def periodic_task():
    result = make_observation_get_request()

    print('\n%s - Fetched %d observations.\n' % (get_time_string(), process_and_publish_observations(result)))


def make_sensors_get_request(max_num_requests=50):
    try:
        session = requests.Session()

        retries = Retry(total=max_num_requests,
                        backoff_factor=0.1,
                        status_forcelist=[500, 502, 503, 504])

        session.mount('http://', HTTPAdapter(max_retries=retries))

        response = session.get(
            url=OBSERVATIONS_GET_ENDPOINT,
            params={'request': 'devices'},
            verify=True)

        return response.json()
    except Exception as e:
        print('Failed to fetch sensors and streams!!!')
        traceback.print_exc()


def process_and_publish_sensors(json_object):
    features = json_object['features']

    for feature in features:
        coord = feature['geometry']['coordinates']
        latitude = coord[0]
        longitude = coord[1]
        id = feature['properties']['traverse_name']

        sensor1 = {
            'id': id + '_SPEED',
            'latitude': latitude,
            'longitude': longitude,
            'quantityKind': 'speed',
            'unit': 'km',
            'topic': TOPIC_ID
        }

        sensor2 = {
            'id': id + '_COUNT',
            'latitude': latitude,
            'longitude': longitude,
            'quantityKind': 'count',
            'unit': 'unit',
            'topic': TOPIC_ID
        }


        response_string = requests.put('http://localhost:14080/api/addsensor', json.dumps(sensor1)).content.decode()
        response_json = json.loads(response_string)

        if response_json['status'] == 'FAIL':
            print("ERROR POSTING SENSOR: " + response_json['cause'])

        response_string = requests.put('http://localhost:14080/api/addsensor', json.dumps(sensor2)).content.decode()
        response_json = json.loads(response_string)

        if response_json['status'] == 'FAIL':
            print("ERROR POSTING SENSOR: " + response_json['cause'])


        response_string = requests.put('http://localhost:14080/api/addstream', json.dumps(stream_from_sensor(sensor1))).content.decode()
        response_json = json.loads(response_string)

        if response_json['status'] == 'FAIL':
            print("ERROR POSTING STREAM: " + response_json['cause'])

        response_string = requests.put('http://localhost:14080/api/addstream', json.dumps(stream_from_sensor(sensor2))).content.decode()
        response_json = json.loads(response_string)

        if response_json['status'] == 'FAIL':
            print("ERROR POSTING STREAM: " + response_json['cause'])


def stream_from_sensor(sensor):
    feature = sensor['quantityKind']

    stream = {
        "id": sensor['id'] + '-' + feature,
        "location":{
            "latitude": sensor['latitude'],
            "longitude": sensor['longitude']
        },
        "streamStart": get_time_string(),
        "generatedBy": sensor['id'],
        "feature": feature
    }
    return stream



def create_topic():
    topic = {
        "name": "topic1",
        "author": "Luis",
        "description": "",
        "databaseName": "",
        "observationDataClassName": "cu.uclv.mfc.enigma.observations.data.custom.DoubleObservationData",
        "interval": 60
    }

    response_string = requests.put('http://localhost:14080/api/addtopic', json.dumps(topic)).content.decode()
    response_json = json.loads(response_string)

    if response_json['status'] == 'FAIL':
        print("ERROR POSTING TOPIC: " + response_json['cause'])
    else:
        st = str(response_json['data'])

        data = json.loads(st.replace('\'', '"'))

        return data['id']



if __name__ == '__main__':
    TOPIC_ID = create_topic()

    print("TOPIC: ", TOPIC_ID)

    process_and_publish_sensors(make_sensors_get_request())

    while True:
        thread = Thread(target=periodic_task, args=())
        thread.start()

        print("\n\n%s - Running Periodic Task...\n\n" % (get_time_string()))

        time.sleep(SLEEP_TIME)
