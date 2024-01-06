# coding=UTF-8

import json
import requests


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

        r = requests.get(self.__endpoint)
        if r.status_code % 100 == 4:
            return

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
