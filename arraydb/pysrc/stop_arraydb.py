#!/usr/bin/env python

import sys
import os


def stop():
    result = os.popen('ps aux | grep python').read()
    strs = result.split('\n')
    for line in strs:
	if './server.py' in line:
	    proc_num = (line.split())[1]
	    cmd = 'kill -9 ' + proc_num
	    print cmd
	    os.system(cmd)


if __name__ == '__main__':
    stop()
