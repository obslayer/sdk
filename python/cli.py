#!/usr/bin/env python3
# coding=UTF-8

"""
实现：
1. 系统级配置 bluepipe.conf
2. 命令级入参 job、table（业务参数）和date（调度参数）
3. 监听SIGTERM和SIGINT杀作业，否则等待作业完成(0/非零)

"""

import argparse
import logging
import signal
import sys
import time
from os.path import abspath, dirname

from lib import bluepipe

__client = bluepipe.from_config_file(
    dirname(abspath(sys.argv[0])))

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s'
)


def signal_handler(signum, frame):
    name = signal.Signals(signum).name
    logging.critical('Got signal %s (%d), killing instances ...', name, signum)
    __client.shutdown(f'signal {name} ({signum})')
    sys.exit(128 + signum)


def to_local_time(value: str):
    if value:
        fmt_offset = [
            ('%Y%m%d%H', 3660),
            ('%Y-%m-%dT%H', 3660),
            ('%Y%m%d', 86460),
            ('%Y-%m-%d', 86460),
        ]

        for fmt, span in fmt_offset:
            try:
                t1 = time.strptime(value, fmt)
                return t1, time.localtime(span + time.mktime(t1))
            except ValueError:
                continue

    return None, None


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-j', '--job',
                        dest='job',
                        help='Job ID from bluepipe',
                        required=True)

    parser.add_argument('-t', '--table',
                        dest='table',
                        help='Full name of source table',
                        required=False)

    parser.add_argument('-f', '--file',
                        dest='file',
                        help='File name of table list',
                        required=False)

    parser.add_argument('-d', '--date',
                        dest='date',
                        help='数据日期，以本地时间YYYYMMDD表示',
                        required=False)

    try:
        config = vars(parser.parse_args())
        offset, margin = to_local_time(config.get('date'))

        tables = []
        if config.get('file'):
            with open(config.get('file'), 'r', encoding='utf-8') as fd:
                for name in fd.read().splitlines():
                    name = name.strip()
                    if name and not name.startswith('#'):
                        tables.append(name)
        else:
            tables.append(config.get('table'))

        if len(tables) < 1:
            parser.print_usage()
            sys.exit(1)

        queued = 0
        for table in tables:
            result = __client.submit(config.get('job'), table, offset, margin)
            if result:
                queued = queued + 1

        if queued < 1:
            sys.exit(2)

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

        if not __client.wait_finished(0):
            sys.exit(3)

    except (argparse.ArgumentError, argparse.ArgumentTypeError):
        parser.print_usage()
        sys.exit(1)
