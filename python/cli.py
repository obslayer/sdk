#!/usr/bin/env python3
# coding=UTF-8

import getopt
import signal
import sys

from lib import bluepipe

__client = bluepipe.from_config_file()


def print_usage():
    print(f'Usage: {sys.argv[0]} -j <job id> -t <table> -c <cursor> -d <done>')
    sys.exit(1)


def signal_handler(signum, frame):
    signame = signal.Signals(signum).name
    print(f'Got signal {signame} ({signum}), killing instances ...')
    __client.shutdown()
    sys.exit(0)


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
        print(command)

        if not ('job' in command) or not ('table' in command):
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
