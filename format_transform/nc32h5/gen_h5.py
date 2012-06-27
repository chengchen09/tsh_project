#!/usr/bin/env python

import sys
import h5py


def gen():
	if len(sys.argv) < 2:
		print "wrong format"
		sys.exit(1)

	h5_fh = h5py.File(sys.argv[1], 'w')
	subgroup = h5_fh.create_group('subgroup')
	ds = subgroup.create_dataset('DS', (4, 5), 'f8')
	ds[...] = 3.14125
	
	h5_fh.close()


if __name__ == '__main__':
	gen()
