#!/usr/bin/env python

import sys
sys.path.append('./gen-py')

from arraydb import ArrayDB
#import arraydb.ArrayDB
from arraydb.ttypes import *
from ArrayDBHandler import *

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer, TNonblockingServer


handler = ArrayDBHandler()
processor = ArrayDB.Processor(handler)
transport = TSocket.TServerSocket(port=9090)
tfactory = TTransport.TFramedTransportFactory()
pfactory = TBinaryProtocol.TBinaryProtocolFactory()

#server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
#server = TNonblockingServer.TNonblockingServer(processor, transport, pfactory)
# You could do one of these for a multithreaded server
#server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory)
server.setNumThreads(1)

print 'Starting the server...'
server.serve()
print 'done.'
