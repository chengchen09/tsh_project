#!/usr/bin/env python

import rtree
import sys
import index


def test_search():
	if len(sys.argv) < 2:
		print 'input error'
		sys.exit(1)

	bbox = (0, 0, 10, 10)
	index.search_index(sys.argv[1], '/z', None, bbox)


if __name__ == '__main__':
	test_search()
