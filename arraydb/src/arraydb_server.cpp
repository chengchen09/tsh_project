/***********************************************************************
* Filename : arraydb_server.cpp 
* Create : chen 2011-11-16
* Created Time: 2011-11-16
* Description: 
* Modified   : 
* **********************************************************************/
#include "ArrayDB.h"
#include <protocol/TBinaryProtocol.h>
#include <server/TSimpleServer.h>
#include <server/TThreadPoolServer.h>
#include <server/TThreadedServer.h>
#include <server/TNonblockingServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>

#include <string>
#include <assert.h>
#include <stdio.h>
#include <iostream>
#include <getopt.h>
#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>
#include "global.h"
#include "ArrayDBHandler.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;
using namespace boost;
using namespace std;
using namespace arraydb;

int main(int argc, char **argv) {
    int c;
    string master;
    string local;
    string data_dir;
    int port = -1;
    int id = -1;
    RoleType role;
    std::vector<ServerConnection> clients;

    while ((c = getopt_long(argc, argv, "l:m:w:i:", NULL, NULL)) >= 0) {
	switch (c) {
	    case 'l':
		int pos;
		local = optarg;
		pos = local.find(":");
		port = lexical_cast<int>(local.substr(pos + 1, local.length() - pos - 1).data());
		break;
	    case 'm':
		master = optarg;
		//clients.push_back(ServerConnection(MASTER, optarg));
		break;
		/* TODO: data_dir */
		/*case 'd':
		  data_dir = optarg;
		  break;*/
	    case 'w':
		clients.push_back(ServerConnection(WORKER, optarg, clients.size() + 1));
		break;
	    case 'i':
		id = lexical_cast<int>(optarg);
		break;
	    default:
		break;
	}
    }

    if (master == local)
	role = MASTER;
    else
	role = WORKER;

    shared_ptr<ArrayDBHandler> handler(new ArrayDBHandler(id, role, clients));
    shared_ptr<TProcessor> processor(new ArrayDBProcessor(handler));
    //shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    //shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
    shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

    TNonblockingServer server(processor, protocolFactory, port);
    printf("Starting the server...\n");
    server.serve();
    printf("sever done\n");
    return 0;
}

/*shared_ptr<TTransportFactory> inputTransportFactory(new TBufferedTransportFactory());
  shared_ptr<TTransportFactory> outputTransportFactory(new TBufferedTransportFactory());
  shared_ptr<TProtocolFactory> inputProtocolFactory(new TBinaryProtocolFactory());
  shared_ptr<TProtocolFactory> outputProtocolFactory(new TBinaryProtocolFactory());*/


/* thread manager */
/*shared_ptr<ThreadManager> threadManager = ThreadManager::newSimpleThreadManager(thread_count);


  shared_ptr<PosixThreadFactory> threadFactory = shared_ptr<ThreadFactory>(new ThreadFactory());
  threadManager->threadFactory(threadFactory);
  threadManager->start();*/


/* simple rpc server: single thread */
//TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);


/* multi-thread server using blocking I/O */
//TThreadedServer server(processor, serverTransport, transportFactory, protocolFactory);*/

/* multi-thread server using non-blocking I/O */
//TNonblockingServer server(processor, protocolFactory, port);
//TNonblockingServer server(processor, inputTransportFactory, outputTransportFactory, inputProtocolFactory, outputProtocolFactory, port);

//TNonblockingServer server(processor, port);


