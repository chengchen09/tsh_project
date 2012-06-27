#!/usr/bin/env python

'''
Author: Chen Cheng
Description: a collection of test for the class CCJson
'''
from ccjson import CCJson
from test_pass1 import TestPass1 
import sys

def err(msg):
    print msg
    sys.exit(1)

def test_get_set_del():
    #object
    cj = CCJson()
    cj['haha'] = '1'
    assert(cj['haha'] == '1')
    del cj['haha']
    try:
        print cj['haha']
    except KeyError:
        pass
    else:
         err('CCJson object del key failed')
    print 'test CCJson object get, put and del operation passed'
    
    #array
    cj = CCJson()
    cj.append(1)
    assert(cj[0] == 1)
    del cj[0]
    try:
        print cj[0]
    except IndexError:
        pass
    else:
        err('CCJson array del element failed')
    print 'test CCJson array get, put and del operation passed'
    
def test_keys():
    cj = CCJson()
    cj['a'] = 'adam'
    cj['b'] = 'bob'
    keys = cj.keys()
    keys.sort()
    if not (keys[0] == 'a' and keys[1] == 'b'):
        err('CCJson object keys() failed')
    cj = CCJson()
    cj.append('1')
    cj.append(2)
    keys = cj.keys()
    if not (keys[0] == 0 and keys[1] == 1):
        err('CCJson array keys() failed')
    print 'test CCJson keys() passed'

def test_update():
    cj = CCJson()
    cj['name'] = 'chen'
    d = {'a': 21, 'name': 'CHEN'}
    cj.update(d)
    if not (cj['a'] == 21 and  cj['name'] == 'CHEN'):
        err('json object update failed')
    cj = CCJson()
    cj.append(1)
    cj.append(2)
    d = {0:11, 1:22}
    cj.update(d)
    if not (cj[0] == 11 and cj[1] == 22):
        err('json array update failed')
    d[4] = 44
    try:
        cj.update(d)
    except IndexError:
        pass
    else:
        err('json array update: index error')
    print 'test CCJson update() passed'

def test_dump_load():
    tt = TestPass1()
    tt.runTest()
    print 'test CCJson dump() load() based on json_test1.py passed'

if __name__ == '__main__':
    test_get_set_del()
    test_keys()
    test_update()
    test_dump_load()
