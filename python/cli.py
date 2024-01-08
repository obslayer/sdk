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

from lib import bluepipe

__client = bluepipe.from_config_file()

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s'
)


def signal_handler(signum, frame):
    signame = signal.Signals(signum).name
    logging.critical('Got signal %s (%d), killing instances ...', signame, signum)
    __client.shutdown(f'signal {signame} ({signum})')
    sys.exit(128 + signum)


def to_unix_epoch(value):
    # Z
    return int(time.strftime('%s', time.strptime(value, '%Y%m%d')))


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument('-j', '--job',
                        dest='job',
                        help='bluepipe作业ID',
                        required=True)

    parser.add_argument('-t', '--table',
                        dest='table',
                        help='bluepipe来源表名',
                        required=True)

    parser.add_argument('-d', '--date',
                        dest='date',
                        # 暂时只支持daily run
                        help='数据日期，以本地时间YYYYMMDD表示',
                        required=False,
                        default=time.strftime('%Y%m%d', time.localtime(time.time() - 86400))
                        )

    try:
        config = vars(parser.parse_args())
        offset = 1000 * to_unix_epoch(config.get('date'))

        resp = __client.submit(config.get('job'), config.get('table'),
                               offset, offset + 86460000)
        if not resp:
            sys.exit(2)

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

        if not __client.wait_finished(0):
            sys.exit(3)

    except (argparse.ArgumentError, argparse.ArgumentTypeError):
        parser.print_usage()
        sys.exit(1)
