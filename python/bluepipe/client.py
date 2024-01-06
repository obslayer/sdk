# coding=UTF-8

import json
from os.path import join, dirname, expanduser
from sys import argv


# import requests

def __load_config():
    search_paths = [
        join(dirname(argv[0]), 'bluepipe.conf'),
        join(expanduser('~'), 'bluepipe.conf'),
        '/etc/bluepipe.conf',
        join(dirname(argv[0]), 'bluepipe.default.conf')
    ]

    for filename in search_paths:
        try:
            with open(filename) as config:
                output = {}
                for x in config.read().strip().splitlines():
                    if not x.startswith('#'):
                        pair = x.split('=')
                        output[pair[0].strip()] = pair[1].strip()
                return output
        except OSError:
            continue

    return {}


def from_config_file():
    config = __load_config()
    return HttpClient(
        config.get('endpoint', 'https://api.1stblue.com/api/v1'),
        config.get('accessId', ''),
        config.get('accessKey', '')
    )


class HttpClient:
    __endpoint = ""
    __accessId = ""
    __accessKey = ""

    def __init__(self, endpoint, accessId, accessKey):
        self.__endpoint = endpoint.rstrip().rstrip('/')
        self.__accessId = accessId
        self.__accessKey = accessKey

    def __http_call(self, method, prefix, payload):
        if payload:
            json.dumps(payload)

    #  r = requests.get(self.__endpoint)
    #  if r.status_code % 100 == 4:
    #      return

    def submit(self, job_id, table, offset, done_mark):
        self.__http_call('POST', '/job/%s/start', {
            'offset': offset,
            'tables': table,
            'done': done_mark
        })

    def get_status(self, instance):
        self.__http_call('GET', '/instance/%s/status', None)

    def kill(self, instance):
        self.__http_call('POST', '/instance/%s/stop', None)
