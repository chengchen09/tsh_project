#!/usr/bin/env python

import sys
import os
import time

chunk_size_list = ['128', '256', '512', '1024']
etop_conditions = ['-10215', '-9575', '-8505', '-6665', '-5876', '-5080', '-4054', '-2457', '-10', '565', '8000']
pres_conditions = ['51040', '51443', '52191', '54095', '58800', '67200', '84390', '93888', '97597', '100018', '104800']
fraction = ['1e-6', '1e-5', '1e-4', '0.001', '0.01', '0.1', '0.3', '0.5', '0.7', '0.9', '1.0']
# small conditions
#etop_conditions = ['-10215', '-9575', '-8505', '-6665', '-5876', '-5080']
#pres_conditions = ['51040', '51443', '52191', '54095', '58800', '67200']
#fraction = ['1e-6', '1e-5', '1e-4', '0.001', '0.01', '0.1']

# large conditions
#fraction = ['0.1', '0.2', '0.3', '0.4', '0.5', '0.6', '0.7', '0.8', '0.9', '1.0']
#etop_conditions = ['-5080', '-4532', '-4054', '-3484', '-2457', '-432', '-10', '198', '565', '8000']
#pres_conditions = ['67200', '74003', '84390', '89867', '93888', '96087', '97597', '98871', '100018', '11000']

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

	    #start_server()
	    #set_index_cmd = client + ' \"set indextype ' + index_type + '\"' 
    	    #print set_index_cmd
	    #os.popen(set_index_cmd)

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
    
    # remove array data
    #cmd = 'rm ../worker-1/' + arr_name + '/*'
    #os.system(cmd)

    #create_cmd = ''
    #store_cmd = ''
    conditions = []
    
    if arr_name == 'etop':
	#create_cmd = client + ' \"create array ' + arr_name + ' <a:float> [x=0:10801,' + chunk_size + ',0, y=0:21601,' + chunk_size + ',0]\"'
	#store_cmd = client + ' \"store etop /home/bus/chen/etopo1_bed_g_f4.flt\"'
	conditions = etop_conditions
    elif arr_name == 'pres':
	#create_cmd = client + ' \"create array ' + arr_name + ' <a:float> [x=0:17520,' + chunk_size + ',0, y=0:22085,' + chunk_size + ',0]\"'
	#store_cmd = client + ' \"store pres /home/bus/chen/data/pres.bin\"'
	conditions = pres_conditions
   
    select_cmd = client + ' \"select ' + arr_name + ' from ' + arr_name + ' where ' + arr_name + ' < '
    
    # set nthreads 16
    set_cmd = client + ' \"set nthreads 16\"'
    os.system(cmd)

    # create index cmd
    crt_index_cmd = client + ' \"create index index_name ' + index_type + ' on ' + arr_name + '\"'
    
    # create array, store array, create index
    start_server()
    #print create_cmd
    #os.popen(create_cmd)
    #print store_cmd
    #os.system(store_cmd)
    print crt_index_cmd
    os.system(crt_index_cmd)
    slt_cmd = select_cmd + conditions[0] + '\"'
    set_index_cmd = client + ' \"set indextype ' + index_type + '\"' 
    os.popen(set_index_cmd)
    print slt_cmd
    os.popen(slt_cmd)

    # get the indexsize
    
    #stop_server()

    response_time = []
    select(conditions, select_cmd, index_type, response_time, log)
    
    # write result
    for i in range(len(fraction)):
	(all, io) = response_time[i]
	result.writelines(str(i) + ' ' + fraction[i] + ' ' + str(io) + ' ' + str(all) + '\n')

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
    if len(sys.argv) >= 3:
	check_compress_type(compress_type)
	compress_type = '_' + sys.argv[3]
	print compress_type

    arr_name = sys.argv[1]
    index_type = sys.argv[2]
    check_index_type(index_type)

    record_performance(arr_name, index_type, compress_type)


