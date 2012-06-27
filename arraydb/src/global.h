/***********************************************************************
 * Filename : global.h
 * Create : XXX 2011-09-11
 * Created Time: 2011-09-11
 * Description: 
 * Modified   : 
 * **********************************************************************/
#ifndef _GLOBAL_H
#define _GLOBAL_H

#include "ArrayDB.h"
#include "cc_part.h"

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
#include <float.h>
#include <limits.h>

#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string.hpp>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;
using namespace boost;
using namespace std;
using namespace arraydb;


namespace arraydb {
    enum RoleType {
	MASTER = 0,
	WORKER
    };

    enum ArrayDBError {
	SUCCESS = 0,
	ARRAY_NOT_EXIST,    /* find_arrinfo can't find the array from name */
	SUBARRAY_PARAMS_ERROR,	    /* num of parameters of subarray is not appropriate */
	COORDS_OVERFLOW,	    /* subarray coordinators over the array coords */
	CHUNK_DATA_NOT_FIND,	/* chunk data file can't be find */
	COMPRESS_FAILED,    /* compress chunk data error */
	DECOMPRESS_FAILED,  /* decompress chunk data error */
	UNKNOWN
    };

    enum CompareType {
	LESS = 0,
	LARGER,
	LESS_LARGER
    };

    enum ChunkType {
	CANDIDATE_CHUNK = 0,
	VALID_CHUNK,
	INVALID_CHUNK,
    };
    
    typedef struct _chunk_type {
	int id;
	ChunkType type;
    } chunk_type_info_t;

    /*enum IndexType {
      CHUNK_INDEX = 0,
      BINARY_INDEX
      };*/

    /*typedef struct _diminfo {
	string name;
	size_t length;
	size_t chunk_length;
	size_t overlap_length;
    } Dim_Info;

    typedef struct _chunkinfo {
	int chunk_id;
	int node_id;
	size_t ndims;
	size_t chunk_size;
	std::vector<size_t> first;
	std::vector<size_t> last;
    } Chunk_Info;
    
    typedef struct _arrayinfo {
	int array_id;
	string name;
	string var_name;
	DataType var_type;
	size_t dtype_size;
	size_t nelmts;
	size_t ndims;
	std::vector<DimInfo> dims;
	StorageStrategy ss;
	size_t nchunks;
	std::vector<size_t> chunk_dims;
	std::vector<ChunkInfo> chunks;
    } Array_Info;

    // TODO: man min type should be template
    template <class T>
    typedef struct _chunkindex {
	int chunk_id;
	T max;
	T min;
    } Chunk_Index;

    template <class T>
    typedef struct _indexinfo {
	string chunk_name;
	IndexType index_type;
	std::vector<Chunk_Index<T>> chunkindex;
    } Index_Info;*/

}
#endif
