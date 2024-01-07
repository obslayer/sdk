# coding=UTF-8

import base64
import hmac
import json
import time
from hashlib import sha1
from os.path import join, dirname
from sys import argv

from vendors import requests

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
        except Exception:
            continue

    return {}


def from_config_file():
    config = __load_config()
    return Bluepipe(
        config.get('endpoint', 'https://api.1stblue.com/api/v1'),
        config.get('accessId', ''),
        config.get('accessKey', '')
    )


class Response:
    def __init__(self, resp: requests.Response):
        self.__code = resp.status_code
        self.__message = resp.reason

        if resp.status_code < 400:
            data = resp.json()
            self.__code = data.get('code', self.__code)
            self.__message = data['message']
            self.__success = data['success']
            self.__data = data['data']

    __success = False
    __message = ""
    __code = 0
    __data = None

    def success(self):
        return self.__success

    def code(self):
        return self.__code

    def message(self):
        return self.__message

    def data(self):
        return self.__data


class Bluepipe:
    __endpoint = ""
    __access_id = ""
    __access_key = ""

    __req_timeout = 10

    # 正在运行的instances
    __instances = []

    def __init__(self, endpoint, access_id, access_key):
        self.__endpoint = endpoint.rstrip().rstrip('/')
        self.__access_id = access_id
        self.__access_key = access_key

    def shutdown(self):
        for x in self.__instances:
            self.kill_instance(x)

    def wait_finished(self, timeout=0):
        """
        等待作业完成退出
        :param timeout: 最长等待时间（秒），0代表不限制
        :return: Boolean. True代表正常结束
        """
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
        resp = self.__http_call('POST', f'/job/{job_id}/start', {
            'tables': table,

            # epoch of read / scan cursor
            'offset': offset,

            # epoch to determine if the data is ready
            # 对于CDC作业来讲，commit时间必须大于此值
            # 注意：如果来源库有异步复制（slave replicate），其复制进度也应该超过此阈值
            'threshold': done_mark
        })

        if resp.success():
            print(resp.data())
            self.__instances.append('')

        return resp

    def get_status(self, instance):
        return self.__http_call('GET', f'/instance/{instance}/status')

    def kill_instance(self, instance):
        return self.__http_call('POST', f'/instance/{instance}/stop')

    def __signature(self, value):
        token = hmac.new(self.__access_key.encode('utf-8'), value.encode('utf-8'), sha1)
        return base64.b64encode(token.digest()).decode('utf-8').rstrip('\n')

    def __http_call(self, method, prefix, payload=None):

        queries = {
            'SignatureNonce': time.time(),
        }

        headers = {
            'Date': time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime()),
            'User-Agent': __version__,
            'Content-Type': 'application/json'
        }

        if payload:
            payload = json.dumps(payload).encode('utf-8')
            headers['Content-Length'] = str(len(payload))

        token = []
        for key, value in headers.items():
            key = key.lower()
            if 'date' == key or key.startswith('x-'):
                token.append(f'{key}:{value}')

        token.sort()
        token = [f'{method} {prefix}'] + token
        if payload:
            token.append('')
            token.append(payload.decode('utf-8'))

        signature = self.__signature('\n'.join(token))
        headers['Authorization'] = f'AKEY {self.__access_id}:{signature}'

        resp = requests.request(method, self.__endpoint + prefix,
                                params=queries,
                                data=payload,
                                headers=headers,
                                timeout=self.__req_timeout,
                                )
        return Response(resp)
