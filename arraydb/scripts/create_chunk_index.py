#!/usr/bin/env python

import sys
import os
import subprocess

chunk_size_list = ['128', '256', '512', '1024']

log = None

def usage():
    print './create_chunk_index.py array-name'


def execute(log, result, arr_name, chunk_size):
  
    log.writelines('\nchunk_size: ' + chunk_size + '\n')
    # remove array data
    cmd = 'rm ../worker-1/' + arr_name + '/*'
    os.system(cmd)

    client = '../src/simple_client'
    create_cmd = ''
    store_cmd = ''

    if arr_name == 'etop':
	create_cmd = client + ' \"create array ' + arr_name + ' <a:float> [x=0:10801,' + chunk_size + ',0, y=0:21601,' + chunk_size + ',0]\"'
	store_cmd = client + ' \"store etop /home/bus/chen/etopo1_bed_g_f4.flt\"'

    elif arr_name == 'pres':
	create_cmd = client + ' \"create array ' + arr_name + ' <a:float> [x=0:17520,' + chunk_size + ',0, y=0:22085,' + chunk_size + ',0]\"'
	store_cmd = client + ' \"store pres /home/bus/chen/data/pres.bin\"'
    
    select_cmd = client + ' \"select ' + arr_name + ' from ' + arr_name + ' where ' + arr_name + ' < '

    os.popen(create_cmd)
    os.system(store_cmd)

    index_cmd = client + ' \"create index index_name chunk_index on ' + arr_name + '\"'
    log.writelines(index_cmd + '\n')
    retry = 8
    records = []
    for i in range(retry):
	#remove chunk index
	rm_cmd = 'rm ../worker-1/' + arr_name + '.chunk_index'
	os.system(rm_cmd)
	
	os.system('echo 3 > /proc/sys/vm/drop_caches')
	os.popen('free -m')

	res = os.popen(index_cmd).read()
	print res
	
	log.writelines(str(i) + '\n' + res)

	pos = res.find('time: ')
	pos2 = res.find('\n', pos)
	tstr = res[pos + 6: pos2]
	records.append(float(tstr))
	 
    records.sort()
    log.writelines('after sorted:\n')
    for time in records:
	log.writelines(str(time) + '\n')

    avg = 0
    for i in range(2, 6):
	avg += records[i]
    avg /= 4
	
    log.writelines('avg: ' + str(avg) + '\n\n\n')

    result.writelines(chunk_size + ' ' + str(avg) + '\n')


def record_performance(arr_name): 

    result = open('./' + arr_name + '-index-create.txt', 'w')
    log = open('./' + arr_name + '-index-create.log', 'w')
    
    for chunk_size in chunk_size_list: 
	execute(log, result, arr_name, chunk_size)
    
    log.close()
    result.close()
   

if __name__ == '__main__':
    if len(sys.argv) < 2:
	usage()
	sys.exit(1)

    arr_name = sys.argv[1]
    record_performance(arr_name)

