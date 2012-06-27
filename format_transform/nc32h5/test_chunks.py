#!/usr/bin/env python

import sys
import h5py


def test_chunk():
	if len(sys.argv) < 2:
		print 'format error'
		sys.exit(1)

	fh = h5py.File(sys.argv[1], 'r')

	for obj_h in fh.listobjects():
		print obj_h.name
		print obj_h.chunks

if __name__ == '__main__':
	test_chunk()
