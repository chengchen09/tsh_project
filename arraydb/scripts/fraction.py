#!/usr/bin/python

import sys
import os
from data import fractions, relative_errors

fractions = ['1e-6', '1e-5', '1e-4', '0.001', '0.01', '0.1', '0.3', '0.5', '0.7', '0.9', '1.0']

def usage():
    print 'Usage: fraction array_name min max'


def fraction(arr_name, min, max):
    
    log = open('./' + arr_name + '_fraction.txt', 'w')
    #res = open('./' + arr_name + '_fraction.txt', 'w')

    thresholds = []
    client = '../src/simple_client'
    cmd = client + ' \"select ' + arr_name + ' from ' + arr_name + ' where ' + arr_name + ' < '
    newfractions = fractions[:len(fractions)-1]
    print newfractions
    index = -1
    for frac in newfractions:
	index += 1
	curf = 0
	low = min
	mid = min
	high = max
	f = float(frac)
	
	res = ''
	select = ''
	while abs(curf - f) >= relative_errors[index]:
	    if curf > f:
		high = mid
	    else:
		low = mid
	    mid = (high + low) / 2
	    select = cmd + str(mid) + '\"'
	    res = os.popen(select).read()
	    pos = res.find('ratio: ')
	    pos2 = res.find('\n', pos)
	    curf = float(res[pos + 7: pos2])

	print select
	print res;
	thresholds.append(mid)
	log.writelines(frac + '\n' + select + '\n' + res + '\n')
	
    fstr = '['
    for t in thresholds:
	fstr += '\'' + str(t) + '\', '
    fstr += '\'' + str(max) + '\']'
    log.write(fstr)
    log.close()

if __name__ == '__main__':
    if len(sys.argv) < 4:
	usage()
	sys.exit(1)

    min = float(sys.argv[2])
    max = float(sys.argv[3])
    fraction(sys.argv[1], min, max)  
