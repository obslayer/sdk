# coding=UTF-8

import json
import time
from os.path import join, dirname
from sys import argv


def __load_config():
    search_paths = [
        join(dirname(argv[0]), 'bluepipe.conf'),
        '/etc/bluepipe.conf',
        join(dirname(argv[0]), 'config.default.conf')
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
    return Bluepipe(
        config.get('endpoint', 'https://api.1stblue.com/api/v1'),
        config.get('accessId', ''),
        config.get('accessKey', '')
    )


class Bluepipe:
    __endpoint = ""
    __accessId = ""
    __accessKey = ""

    # 正在运行的instances
    __instances = []

    def __init__(self, endpoint, accessId, accessKey):
        self.__endpoint = endpoint.rstrip().rstrip('/')
        self.__accessId = accessId
        self.__accessKey = accessKey

    def shutdown(self):
        for x in self.__instances:
            self.kill(x)

    def wait_finished(self, timeout=0):
        expire = time.time() + timeout
        while len(self.__instances) > 0:
            for x in self.__instances:
                resp = self.get_status(x)
                if resp in ['FINISHED', 'KILLED', 'FAILED']:
                    try:
                        self.__instances.remove('')
                    except ValueError:
                        continue

            if len(self.__instances) > 0:
                if timeout > 0 and time.time() >= expire:
                    return False

                time.sleep(3)

        return True

    def submit(self, job_id, table, offset, done_mark):
        self.__http_call('POST', '/job/{}/start'.format(job_id), {
            'offset': offset,
            'tables': table,
            'done': done_mark
        })
        self.__instances.append('abcd')

    def get_status(self, instance):
        self.__http_call('GET', '/instance/{}/status'.format(instance))
        return 'FINISHED'

    def kill(self, instance):
        self.__http_call('POST', '/instance/{}/stop'.format(instance))

    def __http_call(self, method, prefix, payload=None):
        if payload:
            json.dumps(payload)

    #  r = requests.get(self.__endpoint)
    #  if r.status_code % 100 == 4:
    #      return
