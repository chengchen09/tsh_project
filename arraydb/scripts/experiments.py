#!/usr/bin/env python

import sys
import os
import subprocess

chunk_size_list = ['128', '256', '512', '1024']
etop_conditions = ['-10215', '-9575', '-8505', '-6665', '-5876', '-5080', '-4054', '-2457', '-10', '565', '8000']
pres_conditions = ['51040', '51443', '52191', '54095', '58800', '67200', '84390', '93888', '97597', '100018', '104800']
shum_conditions = ['-0.000077', '-0.00000165', '-0.0000001845', '-0.00000001585', '0.0000000096', '0.0000112', '0.000555', '0.003135', '0.006727', '0.01566', '0.05']
fraction = ['1e-6', '1e-5', '1e-4', '0.001', '0.01', '0.1', '0.3', '0.5', '0.7', '0.9', '1.0']

log = None

def usage():
    print './experiments array-name'


def start_server():
    curdir = os.getcwd()

    os.chdir('../worker-1')
    os.system('./launch_arraydb.py worker-1 1>/dev/null &')

    os.chdir('../master')
    os.system('./launch_arraydb.py master 1>/dev/null &')
    
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

    print 'stop sever'


def select(log, conditions, select_cmd, is_no_index, no_index_time, chunk_index_time):

    if is_no_index == 1:
	retry = 10
    else:
	retry = 12
    
    for c in conditions:
	tmp = select_cmd + c + '\"'
	print tmp 

	log.writelines(tmp + '\n')
	records = []
	for i in range(retry):
	    # clear os buffer
	    res = os.popen(select_cmd + str(conditions[-1]) + '\"').read()
	    os.system('echo 3 > /proc/sys/vm/drop_caches')
	    os.popen('free -m')

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
	    #records.append(float(tstr))
	    records.append((all_time, io_time))
	new_records = sorted(records)

	log.writelines('after sorted:\nio_time all_time\n')
	for (all_time, io_time) in new_records:
	    log.writelines(str(io_time) + ' ' + str(all_time) + '\n')

	if is_no_index == 1:
	    avg_all = 0
	    avg_io = 0
	    for i in range(3, 7):
	        (all, io) = new_records[i]
		avg_all += all
		avg_io += io
	    avg_all /= 4
	    avg_io /= 4
	    no_index_time.append((avg_all, avg_io))
	else:
	    avg_all = 0
	    avg_io = 0
	    for i in range(3, 9):
	        (all, io) = new_records[i]
		avg_all += all
		avg_io += io

	    avg_all /= 6 
	    avg_io /= 6
	    chunk_index_time.append((avg_all, avg_io))

	log.writelines('avg_io: ' + str(avg_io) + '\tavg_all: ' + str(avg_all) + '\n')


def execute(log, result, arr_name, chunk_size):
   
    # remove array data
    #cmd = 'rm ../worker-1/' + arr_name + '/*'
    #os.system(cmd)

    client = '../src/simple_client'
    create_cmd = ''
    store_cmd = ''

    conditions = []
    rarr_name = arr_name + '_' + chunk_size
    
    if arr_name == 'etop':
	rarr_name = arr_name + '_' + a
	create_cmd = client + ' \"create array ' + rarr_name + ' <a:float> [x=0:10801,' + chunk_size + ',0, y=0:21601,' + chunk_size + ',0]\"'
	store_cmd = client + ' \"store ' + rarr_name + ' /home/bus/chen/etopo1_bed_g_f4.flt\"'
	conditions = etop_conditions

    elif arr_name == 'pres':
	create_cmd = client + ' \"create array ' + rarr_name + ' <a:float> [x=0:17520,' + chunk_size + ',0, y=0:22085,' + chunk_size + ',0]\"'
	store_cmd = client + ' \"store ' + rarr_name + ' /home/bus/chen/data/pres.bin\"'
	conditions = pres_conditions
    elif arr_name == 'shum':
	create_cmd = client + ' \"create array ' + rarr_name + ' <a:float> [x=0:17520,' + chunk_size + ',0, y=0:22085,' + chunk_size + ',0]\"'
	store_cmd = client + ' \"store ' + rarr_name + ' /home/bus/chen/data/shum.bin\"'
	conditions = shum_conditions
    
    select_cmd = client + ' \"select ' + rarr_name + ' from ' + rarr_name + ' where ' + rarr_name + ' < '

    os.system(client + ' \"set threadpoll off\"')
    #os.popen(create_cmd)
    #os.system(store_cmd)

    no_index_time = []
    chunk_index_time = []
    
    # no index
    log.writelines(client + ' \"set indextype no_index\"\n')
    is_no_index = 1
    os.popen(client + ' \"set indextype no_index\"')
    select(log, conditions, select_cmd, is_no_index, no_index_time, chunk_index_time)
    
    # chunk index
    log.writelines('\n\n' + client + ' \"set indextype chunk_index\"\n')
    is_no_index = 0
    cmd = client + ' \"create index index_name chunk_index on ' + rarr_name + '\"'
    os.popen(cmd)
    os.popen(client + ' \"set indextype chunk_index\"')
    select(log, conditions, select_cmd, is_no_index, no_index_time, chunk_index_time)

    # write result
    for i in range(len(fraction)):
	(all1, io1) = no_index_time[i]
	(all2, io2) = chunk_index_time[i]
	result.writelines(str(i) + ' ' + fraction[i] + ' ' + str(io1) + ' ' + str(all1) + 
		' ' + str(io2) + ' ' + str(all2) + '\n')


def record_performance(arr_name): 

    start_server()
    for chunk_size in chunk_size_list: 
	log = open('./' + arr_name + '-' + chunk_size + '.log', 'w')
	result = open('./' + arr_name + '-' + chunk_size + '.txt', 'w')
	execute(log, result, arr_name, chunk_size)
	result.close()
	log.close()
   
    stop_server()


if __name__ == '__main__':
    if len(sys.argv) < 2:
	usage()
	sys.exit(1)

    arr_name = sys.argv[1]
    #index_type = sys.argv[2]
    record_performance(arr_name)


