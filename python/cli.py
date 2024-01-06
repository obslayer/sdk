#!/usr/bin/env python3
# coding=UTF-8

import signal
import sys
import time
from bluepipe import client

"""

Usage:
  cli -job {} -table {} -offset -check []
"""

__client = client.from_config_file()
__instances = []

def signal_handler(signum, frame):
    signame = signal.Signals(signum).name
    print(f'Got signal {signame} ({signum}), killing instances...')
    for id in __instances:
        __client.kill(id)
    sys.exit(0)

if __name__ == "__main__":

    __client.submit("", "", 0, 0)

    id="abcd"

    __instances.append(id)

    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    while len(__instances) > 0:
        for id in __instances:
            __client.get_status(id)
            # code == 4XX || != FINISHED
            #__instances.remove(id)

        if len(__instances) > 0:
            time.sleep(3)
