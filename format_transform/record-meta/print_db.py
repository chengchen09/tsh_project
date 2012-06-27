#!/usr/bin/python
import bsddb3.db as bsddb
from record_config import *

def test_record():
	records = []
		
	# print data
	dbh = bsddb.DB()
	dbh.set_flags(bsddb.DB_DUPSORT)
	dbh.open(dbpath, dbtype=bsddb.DB_BTREE, flags=bsddb.DB_CREATE)
	
	values = []
	cur = dbh.cursor()
	val = cur.first()
	while val is not None:
		print val
		values.append(val[1])
		val = cur.next()
	
	cur.close()
	dbh.close()

if __name__ == '__main__':
	test_record()
