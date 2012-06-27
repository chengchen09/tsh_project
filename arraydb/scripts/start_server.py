#!/usr/bin/env python
import os
import time

def start_server():
    curdir = os.getcwd()

    os.chdir('../worker-1')
    os.system('./launch_arraydb.py worker-1 1>/dev/null &')

    os.chdir('../master')
    os.system('./launch_arraydb.py master 1>/dev/null &')
    
    os.chdir(curdir)
    print 'start sever'


if __name__ == '__main__':
    start_server()
