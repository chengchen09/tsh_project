#!/usr/bin/python
"""record the metadata of input file"""

import bsddb3.db as bsddb
import os
import logging
import sys
import getopt
from record_config import *

#dbpath = 'record.db'
#logfile = '/tmp/record.log'
#
#NC3 = 1
#NC4 = 2
#HDF5 = 3
#GRIB2 = 4
#TEXT = 5
#BIN = 6
#HDF4 = 7


def record_meta(dbfile, records):
	dbh = bsddb.DB()
	dbh.set_flags(bsddb.DB_DUPSORT)
	dbh.open(dbfile, dbtype=bsddb.DB_BTREE, flags=bsddb.DB_CREATE)
	
	for (key, value) in records:
	# put data
		try:
			dbh.put(key, value, flags=bsddb.DB_NODUPDATA)
		except bsddb.DBKeyExistError:
			# print 'catch DBKeyExistError'
			pass
	dbh.close()


#TODO 
def allocate_file_path(datapath):
    return '/tmp/data/' + os.path.basename(datapath) + '.nc4'


def parse_file(fpath, records, datapaths):
	with open(fpath, 'r') as fh:
		for line in fh:
			strs = line.split()
			if 0 == len(strs):
				pass
			elif 1 == len(strs):
				logging.warning('%s: there is only one elements presented', strs[0])
				pass
			else:
				path = allocate_file_path(strs[0])
				datapaths.append((strs[0], path))
				for tab in strs[1:]:
					records.append((tab, path))
					#print tab, strs[0]
#TODO:
def convert_data(origin_type, datapaths):
	if NC3 == origin_type:
		for (indata, outdata) in datapaths:
			print indata, outdata
			

def record_data(fpath):
	# TODO: write log
	records = []
	datapaths = []
	parse_file(fpath, records, datapaths)
	record_meta(dbpath, records)
	convert_data(origin_type, datapaths)


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

#TODO
def usage():
	print 'usage is not implemented yet'	
	sys.exit(1)

def main():
	if len(sys.argv) < 2:
		usage()
	
	try:
		opts, args = getopt.gnu_getopt(sys.argv[1:], 'hH', ['help', 'nc3', 'nc4', 'hdf5', 'grib2', 'text', 'bin'])
	except getopt.GetoptError, err:
		print str(err)
		usage()
	
	logging.basicConfig(filename=logfile, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S', filemode='a', level=logging.DEBUG)
	origin_type = NC3 
	for o, a in opts:
		if o in ('-h', '-H', '--help'):
			usage()
		elif o == '--nc3':
			origin_type = NC3
		elif o == '--nc4':
			origin_type = NC4
		elif o == '--hdf5':
			origin_type = HDF5
		elif o == '--grib2':
			origin_type = GRIB2
		elif o == '--text':
			origin_type = TEXT
		elif o == '--bin':
			origin_type = BIN
		elif o == '--hdf4':
			origin_type = HDF4
		else:
			print "unsupport option:", o
			usage()
	
	fpath = args[0]
	records = []
	datapaths = []
	parse_file(fpath, records, datapaths)
	record_meta(dbpath, records)
	convert_data(origin_type, datapaths)


if __name__ == '__main__':
	#logging.basicConfig(filename=logfile, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S', filemode='a', level=logging.DEBUG)
    #record_data(sys.argv[1])
    #test_record()
	main()
