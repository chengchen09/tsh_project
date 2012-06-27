/***********************************************************************
 * Filename : simple_client.cpp
 * Create : XXX 2011-12-13
 * Created Time: 2011-12-13
 * Description: 
 * Modified   : 
 * **********************************************************************/
#include <stdio.h>
#include <unistd.h>
#include <sys/time.h>
#include <cstring>
#include <iostream>

#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>
#include <boost/algorithm/string.hpp>
#include "ArrayDB.h"

using namespace std;
using namespace apache::thrift;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;

using namespace arraydb;

using namespace boost;

#define MAX_REQUEST_SIZE 1000

int main(int argc, char** argv) {
    if (argc < 2) {
	printf("arguments fault\n");
	return -1;
    }

    shared_ptr<TTransport> socket(new TSocket("localhost", 9090));
    shared_ptr<TTransport> transport(new TFramedTransport(socket));
    shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    ArrayDBClient client(protocol);
    std::vector<string> strs;
    string response;
    string request(argv[1]);

    try {
	transport->open();

	
	/* remove space, \t and ; from leading and trial */
	trim_if(request, is_any_of(" \t"));	    

	string *data;

	data = new string("");

	if (istarts_with(request, "store")) {
	    FILE *fh;
	    size_t size, bytes_read;

	    boost::split(strs, request, is_any_of(" \t"), token_compress_on);
	    if (strs.size() < 3) {
		cout<<"Usage: store name infile\n";	    
		return -1;
	    }

	    /* read file */
	    fh = fopen(strs[2].data(), "rb");
	    assert(fh != NULL);
	    fseek(fh, 0, SEEK_END);
	    size = ftell(fh);
	    fseek(fh, 0, SEEK_SET);

	    delete data;
	    data = new string(size, '\0');
	    bytes_read = fread(&((*data)[0]), 1, size, fh);
	    assert(bytes_read == size);
	    fclose(fh);
	}

	client.executeQuery(response, request, *data);
	delete data;
	cout<<response<<endl;

	if (response.size() > 10000) {
	    FILE *fh;
	    fh = fopen("./result.txt", "w");
	    assert(fh != NULL);
	    fwrite(response.data(), 1, response.size(), fh);
	    fclose(fh);
	}
	transport->close();
    } catch (TException &tx) {
	printf("ERROR: %s\n", tx.what());
    }

}
