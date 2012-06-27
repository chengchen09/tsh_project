#!/usr/bin/env python

import sys
sys.path.append('./gen-py')

from arraydb import ArrayDB
from arraydb.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol

try:

    # Make socket
    transport = TSocket.TSocket('localhost', 9090)

    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TFramedTransport(transport)

    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = ArrayDB.Client(protocol)

    # Connect!
    transport.open()

    client.test_nonblocking()
    #client.executeQuery('test from py client', '')
    #print res

    #client.create_arrinfo('')
    # input cmd
    while 1:
	cmd = raw_input('>>>')
	print cmd
	if cmd == 'quit':
	    break

    # Close!
    transport.close()

except Thrift.TException, tx:
    print '%s' % (tx.message)
