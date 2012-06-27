#!/usr/bin/env python

import sys
import os

log = None

def usage():
    print 'Usage: ./bitmap_create array-name'


def excute(arr_name):
    if (arr_name == 'etop'):
	os.chdir('./etop')
    elif (arr_name == 'pres'):
	os.chdir('./pres')
    else:
	print 'wrong array name', arr_name
	sys.exit(1)

    retry = 7

    cmd = "/home/bus/chen/local/bin/ibis -b \"<binning nbins=2000/><encoding interval-equality/>\" -d ."
    print cmd
    log.writelines(cmd + '\n')
   
    records = []
    for i in range(retry):
	print i
	rm_cmd = 'rm ./' + arr_name + '.idx'
	os.system(rm_cmd)

	# drop os cache buf
	os.system('echo 3 > /proc/sys/vm/drop_caches')
	os.popen('free -m')
	#sys.sleep()

	res = os.popen(cmd).read()
	print res
	log.writelines(str(i) + '\n' + res)

	pos = res.find('seconds, ')
	pos2 = res.find(' elapsed', pos)
	tstr = res[pos + 9: pos2]
	records.append(float(tstr))

    records.sort()
    log.writelines('after sorted:\n')
    for time in records:
	log.writelines(str(time) + '\n')

    avg = 0
    for i in range(2, 5):
	avg += records[i]
    avg /= 3
	
    log.writelines('avg: ' + str(avg) + '\n\n\n')


if __name__ == '__main__':

    if len(sys.argv) < 2:
	usage()
	sys.exit(1)

    arr_name = sys.argv[1]
    log = open('./' + arr_name + '-bitmap-create.log', 'w')
    excute(arr_name)
    log.close()
