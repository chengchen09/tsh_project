#!/usr/bin/python
"""search tag in record.db"""

import bsddb3.db as bsddb
import sys
from record_config import *

def search_tag(tag, results):
	records = []
		
	# print data
	dbh = bsddb.DB()
	dbh.set_flags(bsddb.DB_DUPSORT)
	dbh.open(dbpath, dbtype=bsddb.DB_BTREE, flags=bsddb.DB_CREATE)
	
	cur = dbh.cursor()
	val = cur.set(tag)
	while val is not None:
		results.append(val[1])
		val = cur.next_dup()
	
	cur.close()
	dbh.close()

def main():
	if len(sys.argv) < 2:
		print "error"
		sys.exit(1)

	results = []
	search_tag(sys.argv[1], results)
	for res in results:
		print res


if __name__ == '__main__':
	main()

