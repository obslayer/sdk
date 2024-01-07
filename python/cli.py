#!/usr/bin/env python3
# coding=UTF-8

import getopt
import signal
import sys

from lib import bluepipe

__client = bluepipe.from_config_file()

"""
注意这里的‘${dt}’, 会被自动替换成日期，格式为YYYYMMDD, 如20231228.
这个日期的逻辑为：
● 我们配置了每天0点10分调度，比如12月28日的0点10分会生成一个实例
● 因为数据偏移量为0, 所以2月28日0点10分生成实例的数据时间为2月28日0点0分， 如果偏移量为-1, 那么2月28日0点10分生成实例的数据时间为2月27日0点0分
● ‘${dt}‘的值等于数据时间
● ‘${dt}‘支持表达式，如‘${dt-1d}‘
"""


def print_usage():
    print(f'Usage: {sys.argv[0]} -j <job> -t <table> -d <date>')
    sys.exit(1)


def signal_handler(signum, frame):
    signame = signal.Signals(signum).name
    print(f'Got signal {signame} ({signum}), killing instances ...')
    __client.shutdown()
    sys.exit(128 + signum)


# TODO: 根据调度系统传入的参数转换
def to_unix_epoch(value):
    if value.isdigit():
        return int(value)

    return -1


def parse_command(arguments):
    options, args = getopt.getopt(arguments,
                                  'j:t:c:d:',
                                  ['job=',
                                   'table=',
                                   'cursor=',
                                   'done='])
    output = {}
    for opt, arg in options:
        if opt in ['-j', '--job']:
            output['job'] = arg
        elif opt in ['-t', '--table']:
            output['table'] = arg.replace('.', '/')
        elif opt in ['-c', '--cursor']:
            output['cursor'] = to_unix_epoch(arg)
        elif opt in ['-d', '--done']:
            output['done'] = to_unix_epoch(arg)

    return output


if __name__ == "__main__":

    try:
        command = parse_command(sys.argv[1:])
        if ('job' not in command) or ('table' not in command):
            print_usage()

        __client.submit(command.get('job'),
                        command.get('table'),
                        command.get('cursor', -1),
                        command.get('done', -1))

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

        __client.wait_finished(0)

    except getopt.GetoptError:
        print_usage()
