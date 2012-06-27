#!/usr/bin/python

import sys
import os.path
import os

def compress_frag(fpath, nfrag):
	fnames = []
	for i in range(nfrag):
		fnames.append(fpath + '_frag_' + str(i))
		if os.path.isfile(fnames[i]) == False:
			print fnames[i], ' is not exist'
			sys.exit(1)

	for fname in fnames:
		cmd = '../src/lzma_compress ' + fname	
		os.system(cmd)

def Usage():
	print 'Usage: compress_frag infile 4'


if __name__ == '__main__':
	if len(sys.argv) < 3:
		Usage()
		sys.exit(1)

	compress_frag(sys.argv[1], int(sys.argv[2]))
