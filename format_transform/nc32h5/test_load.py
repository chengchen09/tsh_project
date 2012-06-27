#!/usr/bin/env python

import rtree
import sys

def test_search(idx):
	hits = list(idx.intersection((0, 0, 10, 10), objects='raw'))
	for item in hits:
		#print item.id, item.bbox, item.object
		print item


def test_load(index_path):
	idx = rtree.Rtree(index_path)
	test_search(idx)


if __name__ == '__main__':
	#test_create()
	if len(sys.argv) < 2:
		print 'error'
		sys.exit(1)

	test_load(sys.argv[1])
