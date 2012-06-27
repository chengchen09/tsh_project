#!/usr/bin/env python

#import sys
#sys.path.append('./gen-py')

#from arraydb.ttypes import *
import struct

typefmt = ['', 'h', 'H', 'i', 'I', 'q', 'Q', 'f', 'd']

def read(path, type, num):
    """ 
    use: read data from disk, return a list 
    input
	path: the path of file
	type: the data type in the binary file
	num: the number of data items in the binary file
    return
	a tuple cotains the arrays data
    """

    fh = open(path, 'rb')
    bdata = fh.read()
    fh.close()

    assert(type)
    fmt = str(num) + typefmt[type]
    array = struct.unpack(fmt, bdata)

    return array

def write(path, type, num, array):
    """
    use: write data into disk
    """

    assert(type)
    fmt = str(num) + typefmt[type]
    print fmt
    print array
    bdata = struct.pack(fmt, *array)
    
    fh = open(path, 'wb')
    fh.write(bdata)
    fh.close()


if __name__ == '__main__':
    
    array = (1.0, 2.0, 3.0, 4.0)
    type = 7
    num = 4
    path = 'test.bin'

    write(path, type, num, array)

    darray = read(path, type, num)

    print darray
