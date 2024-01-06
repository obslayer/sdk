#!/usr/bin/env python3
# coding=UTF-8

import logging
import signal
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

def main():

    aa = client.HttpClient('https://api.1stblue.com/api/v1',
                           '', '')

    """ kill instances"""
    signal.signal(signal.SIGTERM)

if __name__ == "__main__":
    main()