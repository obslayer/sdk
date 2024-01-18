# coding=UTF-8

import base64
import hmac
import json
import logging
import secrets
import time
from hashlib import sha1
from os.path import join
from urllib.parse import urlparse, parse_qs, quote_plus

import requests as http

__version__ = 'cli-py/0.1.3'


def __load_config(app_home: str) -> dict:
    """
    尝试加载配置文件
    :return:
    """
    search_paths = [
        join(app_home, 'bluepipe.conf'),
        '/etc/bluepipe.conf',
        join(app_home, 'config.default.conf')
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


class Response:
    def __init__(self, resp: http.Response):
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

    def success(self) -> bool:
        return self.__success

    def code(self):
        return self.__code

    def message(self) -> str:
        return self.__message

    def data(self):
        return self.__data


class Bluepipe:
    __endpoint = ""
    __access_id = ""
    __access_key = ""

    __req_timeout = 10

    __logger = logging.getLogger(__name__)
    # 正在运行的instances
    __instances = []

    def __init__(self, endpoint, access_id, access_key):
        self.__endpoint = endpoint.strip().rstrip('/')
        self.__access_id = access_id
        self.__access_key = access_key

    def shutdown(self, message=None) -> None:
        for x in self.__instances:
            self.kill_instance(x, message)

    def wait_finished(self, timeout=0) -> bool:
        """
        等待作业完成退出
        :param timeout: 最长等待时间（秒），0代表不限制
        :return: Boolean. True代表正常结束
        """
        output = False
        expire = time.time() + timeout
        while len(self.__instances) > 0:
            for x in self.__instances:
                status = self.get_status(x) or {}
                banner = status.get('last_status', 'unknown').upper()

                detail = ""
                writen = status.get('total_rows', -1)
                if writen > -1:
                    detail = f': rows={writen}'
                    if status.get('avg_byte_ps', -1) > -1:
                        speed = round(int(status.get('avg_byte_ps')) / 1048576, 2)
                        detail = f'{detail}, {speed} MB/s'

                # total_rows, total_size, avg_rows_ps, avg_byte_ps
                self.__logger.info('instance (%s) %s%s', x, banner, detail)
                if banner in ['FINISHED', 'KILLED', 'FAILED']:
                    self.__instances.remove(x)

                if banner in ['FINISHED']:
                    output = True

            if len(self.__instances) > 0:
                if timeout > 0 and time.time() >= expire:
                    return False

                time.sleep(3)

        return output

    def submit(self, job_id: str, table: str,
               offset: time.struct_time = None,
               timely: time.struct_time = None) -> (dict, None):

        payload = {
            'tables': table.replace('.', '/'),

            # 以毫秒计的游标值。如果未配置游标列，则不参与数据过滤
            'offset': 0,

            # 以毫秒计的完整阈值，其复制进度也应该超过此阈值
            'timely': 0,
        }
        content = ['job=' + job_id, 'table=' + table]

        if offset:
            payload['offset'] = 1000 * int(time.mktime(offset))
            content.append('offset=' + time.strftime('%Y-%m-%d %H:%M', offset))

        if timely:
            payload['timely'] = 1000 * int(time.mktime(timely))
            content.append('timely=' + time.strftime('%Y-%m-%d %H:%M', timely))

        result = self.__http_call('POST', f'/job/{job_id}/start', None, payload)
        # [{jobId: ***, instanceId:}]
        if not result.success():
            self.__logger.warning('Submit Failed: %s, message=%s', ', '.join(content), result.message())
            return None

        for x in (result.data() or []):
            if not x:
                continue

            instance = x.get('instanceId', '')
            # logview = x.get('logview', '')
            if instance and len(instance) > 0:
                self.__instances.append(instance)
                self.__logger.info('Submit OK: %s, instance=%s', ', '.join(content), instance)

            return result.data()

    def get_status(self, instance) -> (dict, None):
        instance = quote_plus(instance)
        resp = self.__http_call('GET', f'/instance/{instance}/status')
        if resp.success():
            return resp.data()

        return None

    def kill_instance(self, instance, message=None) -> (dict, None):
        instance = quote_plus(instance)
        resp = self.__http_call('POST', f'/instance/{instance}/stop', None, {
            'message': message
        })
        if resp.success():
            return resp.data()

        return None

    @staticmethod
    def __normalize_url(address, extends: dict) -> str:

        origin = urlparse(address)
        params = parse_qs(origin.query)
        for k, v in (extends or {}).items():
            params[k.strip()] = [v.strip()]

        output = []
        for k, v in params.items():
            output.append(quote_plus(k) + '=' + quote_plus(v.pop()))

        if len(output) < 1:
            return origin.path

        output.sort()

        return origin.path + '?' + '&'.join(output)

    def __signature(self, value) -> str:
        token = hmac.new(self.__access_key.encode('utf-8'), value.encode('utf-8'), sha1)
        return base64.b64encode(token.digest()).decode('utf-8').rstrip('\n')

    def __http_call(self, method, address, queries=None, payload=None) -> Response:

        address = self.__normalize_url(address, queries)
        headers = {
            'Accept': 'application/json',
            'Date': time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime()),
            'User-Agent': __version__,
            'Content-Type': 'application/json',
            'X-CA-Key': self.__access_id,
            'X-CA-Nonce': secrets.token_hex(16),
        }

        if payload:
            payload = json.dumps(payload).encode('utf-8')
            headers['Content-Length'] = str(len(payload))

        context = []
        for key, value in headers.items():
            key = key.lower().strip()
            if 'date' == key or key.startswith('x-'):
                context.append(key + ':' + value.strip())

        context.sort()
        context = [method + ' ' + address] + context
        if payload:
            context.append('')
            context.append(payload.decode('utf-8'))

        signature = self.__signature('\n'.join(context))
        headers['Authorization'] = 'APIKEY ' + signature

        return Response(http.request(
            method, self.__endpoint + address,
            params={},
            data=payload,
            headers=headers,
            timeout=self.__req_timeout,
        ))


def from_config_file(app_home: str) -> Bluepipe:
    config = __load_config(app_home)
    return Bluepipe(
        config.get('endpoint', 'https://api.1stblue.com/api/v1'),
        config.get('accessId', ''),
        config.get('accessKey', '')
    )
