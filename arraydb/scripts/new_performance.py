#!/usr/bin/env python

import sys
import os
import time

from data import chunk_size_list, etop_conditions, pres_conditions, shum_conditions, fractions


log = None
client = '../src/simple_client'
retry = 10
abondon = 4

def usage():
    print './performance array-name index_type compress_type(optional)'


def start_server():
    curdir = os.getcwd()

    os.chdir('../worker-1')
    os.system('./launch_arraydb.py worker-1 1>/dev/null &')

    os.chdir('../master')
    os.system('./launch_arraydb.py master 1>/dev/null &')
    
    time.sleep(5)
    os.chdir(curdir)
    print 'start sever'
    

def stop_server():
    result = os.popen('ps aux | grep ./arraydb_server').read()
    strs = result.split('\n')
    for line in strs:
	if '-l' in line:
	    proc_num = (line.split())[1]
	    cmd = 'kill -9 ' + proc_num
	    print cmd
	    os.system(cmd)

    time.sleep(2)
    print 'stop sever'


def select(conditions, select_cmd, index_type, response_time, log):
        
    for c in conditions:
	tmp = select_cmd + c + '\"'
	print tmp 

	log.writelines(tmp + '\n')
	records = []
	for i in range(retry):
	    # clear os buffer
	    os.system('sync')
	    os.system('echo 3 > /proc/sys/vm/drop_caches')

	    # execute select
	    res = os.popen(tmp).read()
	    
	    # get io time:
	    pos = res.find('used:')
	    pos2 = res.find('\n', pos)
	    tstr = res[pos + 6: pos2]
	    io_time = float(tstr)

	    # get time
	    pos = res.find('time:')
	    tstr = res[pos + 6:len(res)-1]
	    all_time = float(tstr)

	    log.writelines(str(i) + ": \n" + res + '\n')
	    records.append((all_time, io_time))
	    #stop_server()

	# sort records according all_time
	new_records = sorted(records)
	
	avg_all = 0
	avg_io = 0
	for i in range(abondon/2, retry - abondon/2):
	    (all, io) = new_records[i]
	    avg_all += all
	    avg_io += io

	avg_all /= retry - abondon
	avg_io /= retry - abondon
	response_time.append((avg_all, avg_io))

	log.writelines('avg_io: ' + str(avg_io) + '\tavg_all: ' + str(avg_all) + '\n')


def execute(arr_name, chunk_size, index_type, compress_type):
   
    # create log and data file
    log = open('./' + arr_name + '_' + index_type + '_' + chunk_size + compress_type + '.log', 'w')
    result = open('./' + arr_name + '_' + index_type + '_' + chunk_size + compress_type + '.txt', 'w')
   
    # set conditions
    conditions = []
    if arr_name == 'etop':
	conditions = etop_conditions
    elif arr_name == 'pres':
	conditions = pres_conditions
    elif arr_name == 'shum':
	conditions = shum_conditions
    else:
	print 'invalid array name: ' + arr_name
	sys.exit(1)

    arr_name = arr_name + '_' + chunk_size + compress_type
    print arr_name
    select_cmd = client + ' \"select ' + arr_name + ' from ' + arr_name + ' where ' + arr_name + ' < '
    crt_index_cmd = client + ' \"create index index_name ' + index_type + ' on ' + arr_name + '\"'
    
    # create array, store array, create index
    start_server()
   
    if compress_type == '':
	print 'set threadpool off'
	os.system(client + ' \"set threadpool off\"')
    # set nthreads 16
    set_cmd = client + ' \"set nthreads 16\"'
    print set_cmd
    os.system(set_cmd)
    print crt_index_cmd
    os.system(crt_index_cmd)
    slt_cmd = select_cmd + conditions[0] + '\"'
    set_index_cmd = client + ' \"set indextype ' + index_type + '\"' 
    os.popen(set_index_cmd)
    print slt_cmd
    os.popen(slt_cmd)

    response_time = []
    select(conditions, select_cmd, index_type, response_time, log)
    
    stop_server()
    # write result
    for i in range(len(fractions)):
	(all, io) = response_time[i]
	result.writelines(str(i) + ' ' + fractions[i] + ' ' + str(io) + ' ' + str(all) + '\n')

    result.close()
    log.close()


def record_performance(arr_name, index_type, compress_type): 
    for chunk_size in chunk_size_list:
    	execute(arr_name, chunk_size, index_type, compress_type)
      

def check_index_type(index_type):
    if index_type == 'no_index':
	pass
    elif index_type == 'chunk_index':
	pass
    elif index_type == 'bitmap_index':
	pass
    else:
	print 'invalid index type: ', index_type
	sys.exit(1)


def check_compress_type(compress_type):
    if compress_type == 'lzma':
	pass
    elif compress_type == 'zlib':
	pass
    else:
	print 'invalid compress type: ', compress_type
	sys.exit(1)


if __name__ == '__main__':
    if len(sys.argv) < 3:
	usage()
	sys.exit(1)

    compress_type = ''
    if len(sys.argv) > 3:
	check_compress_type(sys.argv[3])
	compress_type = '_' + sys.argv[3]
	print compress_type

    arr_name = sys.argv[1]
    index_type = sys.argv[2]
    check_index_type(index_type)

    record_performance(arr_name, index_type, compress_type)

