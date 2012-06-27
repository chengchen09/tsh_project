#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""usage: progname [OPTION] ... inputfile
Example: progname --ph52nc4"""

import sys
import getopt
import os
import time
from multiprocessing import Process
from threading import Thread

import entry

def manager():
	try:
		opts, args = getopt.gnu_getopt(sys.argv[1:], "hH", ["help", "h52nc4", "he2nc3", "hdfeos2nc3"])
	except getopt.GetoptError, err:
		print str(err)
		usage()
	
	# initial
	h52nc4 = 0
	he2nc3 = 0
	
	# parse command line
	for o, a in opts:
		if o in ("-h", "-H", "--help"):
			usage()
		elif o == "--h52nc4":
			h52nc4 = 1
		elif o in ("--he2nc3", "--hdfeos2nc3"):
			he2nc3 = 1
		else:
			assert False, "unhanded option"
	
	start = time.clock()
	print 'start time: ', start	
	if h52nc4 == 1:
		for arg in args:
			nprocs = get_nprocs(arg)
#			p = Process(target=lauch_h52nc4, args=(arg, nprocs))
#			p.start()
			t = Thread(target=lauch_h52nc4, args=(arg, nprocs))
			t.start()
	elif he2nc3 == 1:
		procs = []
		for arg in args:
			nprocs = get_nprocs(arg)
			#lauch_he2nc3(arg, nproc)
			p = Thread(target=lauch_he2nc3, args=(arg, nprocs))
			p.start()
			procs.append(p)
		for p in procs:
			p.join()
		print len(procs)
	else:
		usage()

	end = time.clock()
	print 'end time: ', end
	print 'time used:', end-start 

def usage():
	print __doc__
	sys.exit(-1)

def get_nprocs(arg):
	return 1

def lauch_h52nc4(arg, nprocs):
#	print entry.h52nc4
	print '({0}, {1})'.format(arg, nprocs)
	os.system("/tmp/test")

def lauch_he2nc3(arg, nprocs):
	cmd = entry.he2nc3 + ' ' + arg + ' ' + arg[:len(arg)-3] + 'nc'
	os.system(cmd)

if __name__ == '__main__':
	manager()
