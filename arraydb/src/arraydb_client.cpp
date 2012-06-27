/***********************************************************************
 * Filename : echo_test_client.cpp
 * Create : Chen Cheng 2011-07-15
 * Description: 
 * Modified   : 
 * Revision of last commit: $Rev$ 
 * Author of last commit  : $Author$ $date$
 * licence :
 $licence$
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
    shared_ptr<TTransport> socket(new TSocket("localhost", 9090));
    shared_ptr<TTransport> transport(new TFramedTransport(socket));
    shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
    ArrayDBClient client(protocol);
    std::vector<string> strs;

    try {
	transport->open();

	std::string request, response;
	char query[MAX_REQUEST_SIZE + 1];

	memset(query, 0, MAX_REQUEST_SIZE + 1);

	while(1) {
	    cout<<">> ";
	    cin.getline(query, MAX_REQUEST_SIZE);
	    string request(query);

	    /* remove space, \t and ; from leading and trial */
	    trim_if(request, is_any_of(" \t;")); 	    
	    if (istarts_with(request, "quit"))
		break;
	    /*else if (istarts_with(request, "test_store")) {
		Array array;
		string array_name, path;
		FILE *fh;
		char *buf;
		size_t size, bytes_read;

		boost::split(strs, request, is_any_of(" \t"), token_compress_on);
		if (strs.size() < 3) {
		    cout<<"Usage: store name infile\n";		
		    continue;
		}

		array.name = strs[1];
		path = strs[2];

		// read file
		fh = fopen(path.data(), "rb");
		assert(fh != NULL);
		fseek(fh, 0, SEEK_END);
		size = ftell(fh);
		fseek(fh, 0, SEEK_SET);
		buf = (char *)malloc(size + 1);
		assert(buf != NULL);
		bytes_read = fread(buf, 1, size, fh);
		assert(bytes_read == size);
		fclose(fh);

		array.data.assign(buf, size);	
		client.store(response, array);
		free(buf);
	    }*/
	    else if (istarts_with(request, "echo")) {
		string name;
		boost::split(strs, request, is_any_of(" \t"), token_compress_on);
		if (strs.size() < 2) {
		    cout<<"Usage: echo name\n";
		    continue;
		}
		client.echo(response, strs[1]);
	    }

	    /* executeQuery */
	    else {
		string *data;

		data = new string("");

		if (istarts_with(request, "store")) {
		    FILE *fh;
		    size_t size, bytes_read;

		    boost::split(strs, request, is_any_of(" \t"), token_compress_on);
		    if (strs.size() < 3) {
			cout<<"Usage: store name infile\n";		
			continue;
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
	    }
	    cout<<response<<endl;
	    
	    if (response.size() > 10000) {
		FILE *fh;
		fh = fopen("./result.txt", "w");
		assert(fh != NULL);
		fwrite(response.data(), 1, response.size(), fh);
		fclose(fh);
	    }
	}
	transport->close();
    } catch (TException &tx) {
	printf("ERROR: %s\n", tx.what());
    }

}
