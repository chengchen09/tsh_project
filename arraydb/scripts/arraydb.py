#!/usr/bin/env python

import sys
import os

def usage():
    print 'Usage: arraydb.py start/stop/remote-start/remote-stop'
    sys.exit(1)


def start():
    curdir = os.getcwd()

    os.chdir('../worker-1')
    os.system('./launch_arraydb.py worker-1 1>/dev/null &')

    os.chdir('../worker-2')
    os.system('./launch_arraydb.py worker-2 1>/dev/null &')
    
    os.chdir('../master')
    os.system('./launch_arraydb.py master')


def stop():
    result = os.popen('ps aux | grep ./arraydb_server').read()
    strs = result.split('\n')
    for line in strs:
	if '-l' in line:
	    proc_num = (line.split())[1]
	    cmd = 'kill -9 ' + proc_num
	    print cmd
	    os.system(cmd)

def remote_start():
    # node1
    cmd = "scp arraydb.py arraydb.conf launch_arraydb.py arraydb_server bus@node1:~/chen/worker-1/"
    print cmd
    os.system(cmd)
    cmd = 'ssh node1 "cd ~/chen/worker-1; ./launch_arraydb.py worker-1 1>/dev/null" &'
    os.system(cmd)

    # node2
    cmd = "scp arraydb.py arraydb.conf launch_arraydb.py arraydb_server bus@node2:~/chen/worker-2/"
    print cmd
    os.system(cmd)
    cmd = 'ssh node2 "cd ~/chen/worker-2; ./launch_arraydb.py worker-2 1>/dev/null" &'
    os.system(cmd)

    # master:localhost
    os.system('cp ./arraydb.conf ./arraydb_server launch_arraydb.py ../master')
    os.chdir('../master')
    os.system('./launch_arraydb.py master')


def remote_stop():
    cmd = 'ssh node1 "cd ~/chen/worker-1; ./arraydb.py stop 1>/dev/null" &'
    os.system(cmd)
    cmd = 'ssh node2 "cd ~/chen/worker-2; ./arraydb.py stop 1>/dev/null" &'
    os.system(cmd)
    
    stop()


def main():
    if len(sys.argv) < 2:
	usage()

    if sys.argv[1] == 'start':
	start()
    elif sys.argv[1] == 'stop':
	stop()
    elif sys.argv[1] == 'remote-start':
	remote_start()
    elif sys.argv[1] == 'remote-stop':
	remote_stop()
    else:
	usage()

    
if __name__ == '__main__':
    main()
