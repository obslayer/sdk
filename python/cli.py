#!/usr/bin/env python3
# coding=UTF-8

import logging
import signal
from os.path import join, dirname, expanduser
from sys import argv
import sys
import configparser
from bluepipe import client

"""

Usage:
  cli -c -job {} -table {} -from -min
"""

"""
1. 读（系统级）配置文件, endpoint, accessId, accessKey
2. 读（命令级）参数
3. 提交作业
    | while running
    | sleep
4. 监听系统信号, kill
"""

def read_config():
    search_paths = [
        dirname(argv[0]),
        expanduser('~'),
        '/etc'
    ]

    config_parser = configparser.ConfigParser()
    for directory in search_paths:
        try:
            config_parser.read(join(directory, "bluepipe.ini"))
            return config_parser
        except OSError:
            continue

    config_parser.add_section('openapi')
    config_parser.set('openapi', 'endpoint', 'https://api.1stblue.com/api/v1')
    config_parser.set('openapi', 'accessId', '')
    config_parser.set('openapi', 'accessKey', '')

    return config_parser

__config = read_config()
__client = client.HttpClient(__config.get("openapi", "endpoint"),
                             __config.get("openapi", "accessId"),
                             __config.get("openapi", "accessKey"))
__instances = []

def signal_handler(signum, frame):
    signame = signal.Signals(signum).name
    print(f'Got signal {signame} ({signum}), killing instances...')
    for id in __instances:
        __client.kill(id)
    sys.exit(0)

if __name__ == "__main__":

    __client.submit("", "", 0, 0)

    __instances.push("abcd")

    """ kill instances"""
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    while True:
        __client.get_status("")
        time.sleep(1)
