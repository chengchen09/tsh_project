#!/usr/bin/python

import sys
import os

def usage():
	print "Usage: mcmaster numofprocs infile"
	sys.exit(1)

def main():
	if len(sys.argv) < 3:
		usage()
	
	cmd = 'mpiexec -n ' + sys.argv[1] + ' ./mpi_compress ' + sys.argv[2]
	print cmd
	os.system(cmd)

if __name__ == '__main__':
	main()

