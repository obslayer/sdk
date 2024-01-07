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
    """
    尝试加载配置文件
    :return:
    """
    search_paths = [
        join(dirname(argv[0]), 'bluepipe.conf'),
        '/etc/bluepipe.conf',
        join(dirname(argv[0]), 'config.default.conf')
    ]

    for filename in search_paths:
        try:
            with open(filename, 'r', encoding='utf-8') as config:
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
    __access_id = ""
    __access_key = ""

    # 正在运行的instances
    __instances = []

    def __init__(self, endpoint, access_id, access_key):
        self.__endpoint = endpoint.rstrip().rstrip('/')
        self.__access_id = access_id
        self.__access_key = access_key

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
        self.__http_call('POST', f'/job/{job_id}/start', {
            'offset': offset,
            'tables': table,
            'done': done_mark
        })
        self.__instances.append('abcd')

    def get_status(self, instance):
        self.__http_call('GET', f'/instance/{instance}/status')
        return 'FINISHED'

    def kill(self, instance):
        self.__http_call('POST', f'/instance/{instance}/stop')

    def __signature(self, value):
        token = hmac.new(self.__access_key.encode('utf-8'), value.encode('utf-8'), sha1)
        return base64.b64encode(token.digest()).decode('utf-8').rstrip('\n')

    def __http_call(self, method, prefix, payload=None):

        if payload:
            payload = json.dumps(payload).encode('utf-8')

        # queries = {
        #    'Nonce': time.time(),
        # }

        # GET /aaa?Nonce=...
        token = [f'{method} {prefix}']
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

        # headers
        # Content-Md5
        token.append(req.headers.get('Date'))
        for x in req.headers:
            if x.startswith('X-'):
                print(x)

        # body
        print(token)
        signature = self.__signature('\n'.join(token))
        req.add_header('Authorization', f'AKEY {self.__access_id}:{signature}')

        try:
            with urlopen(req, None, 10) as resp:
                print(resp)
            return None
        except URLError as error:
            print(error)
            return None
