#!/usr/bin/python

import sys;
import os.path;
import os;

def divide(fpath, nfrag):
	size = os.path.getsize(fpath)
	
	frag_size = size / nfrag
	read_size = 0;

	with open(fpath, 'rb') as fh:
		for i in range(nfrag):
			if i != (nfrag - 1):
				frag = fh.read(frag_size)
			else:
				frag = fh.read(size - frag_size * i)
			fname = fpath + '_frag_' + str(i)
			with open(fname, 'wb') as fw:
				fw.write(frag)
			

def usage():
	print "Usage: divide infile 4"


if __name__ == '__main__':
	if len(sys.argv) < 3:
		usage()
		sys.exit(1)
	divide(sys.argv[1], int(sys.argv[2]))
