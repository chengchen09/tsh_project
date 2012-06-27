#!/usr/bin/python

import bsddb3.db as bsddb

dbh = bsddb.DB()
dbh.set_flags(bsddb.DB_DUPSORT)
dbh.open("./tmp.db", dbtype=bsddb.DB_BTREE, flags=bsddb.DB_CREATE)
# put data
try:
	dbh.put('name', 'chen', flags=bsddb.DB_NODUPDATA)
	dbh.put('name', 'wang', flags=bsddb.DB_NODUPDATA)
except bsddb.DBKeyExistError:
	print 'catch DBKeyExistError'

# get data
values = []
cur = dbh.cursor()
val = cur.set('name')
print val
while val is not None:
	values.append(val[1])
	val = cur.next_dup()

print values

cur.close()
dbh.close()
