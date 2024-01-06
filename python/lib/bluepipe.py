# coding=UTF-8

import base64
import hmac
import json
import time
from hashlib import sha1
from os.path import join, dirname
from sys import argv
from urllib.request import Request, urlopen, URLError

__version__ = 'cli-py/0.1.0'


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

    def __signature(self, value):
        token = hmac.new(self.__accessKey.encode('utf-8'), value.encode('utf-8'), sha1)
        return base64.b64encode(token.digest()).decode('utf-8').rstrip('\n')

    def __http_call(self, method, prefix, payload=None):

        if payload:
            payload = json.dumps(payload).encode('utf-8')

        # TODO: nonce防止回放攻击
        queries = {
            'Nonce': time.time(),
        }

        req = Request(
            self.__endpoint + prefix,
            payload,
            {},
            None,
            False,
            method
        )

        # -- Sun, 22 Nov 2015 08:16:38 GMT
        req.add_header('Date', time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime()))
        req.add_header('User-Agent', __version__)
        req.add_header('Content-Type', 'application/json')
        if payload:
            req.add_header('Content-Length', len(payload))

        req.add_header('Authorization', 'AKEY {}:{}'.format(
            self.__accessId, self.__signature('')))
        print(req.headers)

        try:
            resp = urlopen(req, None, 10)
            print(resp)
        except URLError as error:
            print(error)
            return None
