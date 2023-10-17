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

CANT_INSTANCES = 100

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



max_create_topic_time = 0.0
max_publish_sensor_time = 0.0
max_publish_stream_time = 0.0
max_publish_observation_time = 0.0


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


def process_and_publish_observations(json_object, TOPIC_ID):
    global max_publish_observation_time

    data = None

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

            start_time = time.time()

            response_string = requests.put(POST_OBSERVATION_ENDPOINT, count_observation).content.decode()

            finish_time = time.time()

            max_publish_observation_time = max(max_publish_observation_time, finish_time - start_time)

            response_json = json.loads(response_string)

            if response_json['status'] == 'FAIL':
                print("ERROR POSTING COUNT OBSERVATION: " + response_json['cause'])

    return cant_of_observations


def get_time_string():
    return datetime.now().strftime("%Y/%m/%d %H:%M")


def periodic_task(TOPIC_ID):
    global max_publish_observation_time

    result = make_observation_get_request()

    print('\n%s - Fetched %d observations.\n' % (get_time_string(), process_and_publish_observations(result, TOPIC_ID)))

    print("MAX_PUBLISH_OBSERVATION_TIME: " + str(max_publish_observation_time))


def make_sensors_get_request(max_num_requests=50):
    global max_publish_sensor_time, max_publish_stream_time
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


def process_and_publish_sensors(json_object, TOPIC_ID):
    global max_publish_sensor_time, max_publish_stream_time

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

        start_time = time.time()

        response_string = requests.put('http://localhost:14080/api/addsensor', json.dumps(sensor2)).content.decode()

        finish_time = time.time()

        max_publish_sensor_time = max(max_publish_sensor_time, finish_time - start_time)

        response_json = json.loads(response_string)

        if response_json['status'] == 'FAIL':
            print("ERROR POSTING SENSOR: " + response_json['cause'])


        start_time = time.time()
        response_string = requests.put('http://localhost:14080/api/addstream', json.dumps(stream_from_sensor(sensor1))).content.decode()
        finish_time = time.time()

        max_publish_stream_time = max(max_publish_stream_time, finish_time - start_time)

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
    global max_create_topic_time

    topic = {
        "name": "topic1",
        "author": "Luis",
        "description": "",
        "databaseName": "",
        "observationDataClassName": "cu.uclv.mfc.enigma.observations.data.custom.DoubleObservationData",
        "interval": 60
    }

    start_time = time.time()

    response_string = requests.put('http://localhost:14080/api/addtopic', json.dumps(topic)).content.decode()

    finish_time = time.time()

    max_create_topic_time = max(max_create_topic_time, finish_time - start_time)

    response_json = json.loads(response_string)

    if response_json['status'] == 'FAIL':
        print("ERROR POSTING TOPIC: " + response_json['cause'])
    else:
        st = str(response_json['data'])

        data = json.loads(st.replace('\'', '"'))

        return data['id']



def instance():
    TOPIC_ID = create_topic()

    print("TOPIC: ", TOPIC_ID)

    process_and_publish_sensors(make_sensors_get_request(), TOPIC_ID)

    print("MAX_CREATE_TOPIC_TIME: " + str(max_create_topic_time))
    print("MAX_PUBLISH_SENSOR_TIME: " + str(max_publish_sensor_time))
    print("MAX_PUBLISH_STREAM_TIME: " + str(max_publish_stream_time))

    while True:
        thread = Thread(target=periodic_task, args=([TOPIC_ID]))
        thread.start()

        print("\n\n%s - Running Periodic Task...\n\n" % (get_time_string()))

        time.sleep(SLEEP_TIME)



if __name__ == '__main__':

    for  i in range(CANT_INSTANCES):
        thread = Thread(target=instance, args=())
        thread.start()
