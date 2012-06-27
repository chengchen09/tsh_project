#!/usr/bin/env python

import sys
import os

etop_conditions = ['-10215', '-9575', '-8505', '-6665', '-5876', '-5080', '-4054', '-2457', '-10', '565', '8000']
pres_conditions = ['51040', '51443', '52191', '54095', '58800', '67200', '84390', '93888', '97597', '100018', '104800']
fraction = ['1e-6', '1e-5', '1e-4', '0.001', '0.01', '0.1', '0.3', '0.5', '0.7', '0.9', '1.0']
#etop_conditions = ['-10215', '-9575', '-8505', '-6665', '-5876', '-5080']
#pres_conditions = ['51040', '51443', '52191', '54095', '58800', '67200']
log = None

def usage():
    print 'Usage: ./bitmap_result array-name'


def excute(arr_name):
    conditions = []
    if (arr_name == 'etop'):
	conditions = etop_conditions
    elif (arr_name == 'pres'):
	conditions = pres_conditions
    else:
	print 'wrong array name', arr_name
	sys.exit(1)

    retry = 10

    all_time = []
    for c in conditions:
	
	# execute
	#cmd = '/home/bus/chen/local/bin/ibis -d ./d' + arr_name + ' -q "where ' + arr_name + '<=' + c + '"'
	cmd = './test_part ' + arr_name + ' ' + c
	log.writelines(cmd + '\n')
	print cmd
	records = []
	for i in range(retry):
	    # drop os cache bif
	    os.system('echo 3 > /proc/sys/vm/drop_caches')
	    os.popen('free -m')
	    #sys.sleep()
	   
	   
	    res = os.popen(cmd).read() 
	    log.writelines(str(i) + '\n' + res)
	    
	    #pos = res.find('seconds, ')
	    #pos2 = res.find(' elapsed', pos)
	    #tstr = res[pos + 9: pos2]
	    pos = res.find('used:')
	    pos2 = res.find('\n', pos)
	    tstr = res[pos + 6: pos2]
	    records.append(float(tstr))
	
	records.sort()
	log.writelines('after sorted:\n')
	for time in records:
	    log.writelines(str(time) + '\n')

	avg = 0
	for i in range(3, 7):
	    avg += records[i]
	avg /= 4
	
	all_time.append(avg)
	log.writelines('avg: ' + str(avg) + '\n\n\n')

    for i in range(len(fraction)):
	result.writelines(str(i) + ' ' + fraction[i] +' ' + str(all_time[i]) + '\n')


if __name__ == '__main__':

    if len(sys.argv) < 2:
	usage()
	sys.exit(1)

    arr_name = sys.argv[1]
    log = open('./' + arr_name + '-bitmap.log', 'w')
    result = open('./' + arr_name + '-bitmap.txt', 'w')
    excute(arr_name)
    result.close()
    log.close()
