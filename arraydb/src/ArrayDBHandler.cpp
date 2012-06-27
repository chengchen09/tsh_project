/***********************************************************************
 * Filename : ArrayDBHandler.cpp
 * Create : XXX 2011-11-16
 * Created Time: 2011-11-16
 * Description: 
 * Modified   : 
 * **********************************************************************/

#include "ArrayDBHandler.h"
#include "thread_pool.h"

typedef struct _filter_data_arg {
    CompressType::type compress_type;
    char *buf;
    ChunkInfo *chunkinfo;
    ChunkType chunk_type;
    int64_t dtype_size;
    int type;
    double max;
    double min;
    int64_t *count;
    size_t *ncandidate_chunks;
    size_t *nvalid_chunks;
    void *fill_value;
} filterdata_arg_t;

typedef struct _gen_chunk_index_arg {
    CompressType::type compress_type;
    char *buf;
    ChunkInfo *chunkinfo;
    ChunkIndex *chunk_index;
    int64_t dtype_size;
    DataType::type dtype;
    string *fill_value;
} genindex_arg_t;


template <class T>
void *filter_data(void *arg);

void *gen_chunk_index(void *arg);

template <class T>
void serialize(string &str, const T &obj) {
    shared_ptr<TMemoryBuffer> membuf(new TMemoryBuffer());
    shared_ptr<TProtocol> protocol(new TBinaryProtocol(membuf));
    obj.write(protocol.get());
    str.clear();
    str = membuf->getBufferAsString();
}

template <class T>
void deserialize(T &obj, const string& buf) {
    shared_ptr<TMemoryBuffer> membuf(new TMemoryBuffer((uint8_t *)(buf.data()), buf.size()));
    shared_ptr<TProtocol> protocol(new TBinaryProtocol(membuf));
    obj.read(protocol.get());
}


void ArrayDBHandler::create_indexinfo(std::string& _return, const std::string& array_name, const std::string& index_name, const IndexType::type index_type) {
    struct timeval start, end;
    double timeused;

    // time start point
    gettimeofday(&start, NULL);
    if (index_type == IndexType::CHUNK_INDEX)
	create_chunk_index(_return, array_name, index_name);
    else if (index_type == IndexType::BITMAP_INDEX)
	create_bitmap_index(_return, array_name, index_name);
    gettimeofday(&end, NULL);
    
    timeused = (end.tv_sec - start.tv_sec) + (double)(end.tv_usec - start.tv_usec) / 1000000;
    _return += "worker-" + lexical_cast<string>(local_id) + " used time: " + lexical_cast<string>(timeused) + "\n";
   
}

void ArrayDBHandler::create_bitmap_index(std::string& _return, const std::string& array_name, const std::string& index_name) {
    
    string bitstr("");
    ArrayInfo *arrinfo_p;

    arrinfo_p = get_arrinfo(array_name);
    assert(arrinfo_p);
    
    // head
    bitstr += "BEGIN HEADER\nName = \"" + arrinfo_p->name
	+ "\"\nNumber_of_columns = " + lexical_cast<string>(arrinfo_p->nchunks)
	+ "\nNumber_of_rows = " + lexical_cast<string>(arrinfo_p->chunks[0].chunk_size/arrinfo_p->dtype_size)
	+ "\nindex=<binning nbins=2000/><encoding interval-equality/>"
	+ "\nEND HEADER\n";

    // column
    // TODO: datatype
    for (int i = 0; i < arrinfo_p->nchunks; i++) {
	string colstr = "\nBegin Column\nname = \""
	    + arrinfo_p->name + "_" + lexical_cast<string>(i)
	    + "\"\ndata_type = FLOAT"
	    + "\nEnd Colunm\n";
	bitstr += colstr;
    }

    // write to file -part.txt
    string fname;
    FILE *fh;
    fname = "./" + arrinfo_p->name + "/-part.txt";
    fh = fopen(fname.c_str(), "w");
    assert(fh != NULL);
    fwrite(bitstr.c_str(), 1, bitstr.size(), fh);
    fclose(fh);

    // build index
    /*string path = "./" + arrinfo_p->name;
    CCPart part(path.c_str(), static_cast<const char*>(0));
    part.buildIndexes("<binning nbins=2000/><encoding interval-equality/>"); */

    _return = "create bitmap index success\n";
}


void ArrayDBHandler::create_chunk_index(std::string& _return, const std::string& array_name, const std::string& index_name) {
    IndexInfo indexinfo;
    ArrayInfo *arrinfo_p;

    arrinfo_p = get_arrinfo(array_name);
    assert(arrinfo_p);

    indexinfo.array_name = array_name;
    indexinfo.index_name = index_name;
    indexinfo.index_type = IndexType::CHUNK_INDEX;

    if (use_thread_pool) {
	size_t count, bufsize;
	genindex_arg_t *targ;
	ChunkIndex chunk_index;
	char *buf;
	int pos;

	pos = 0;
	count = 0;
	chunk_index.chunk_id = -1;
	chunk_index.max = -1;
	chunk_index.min = -1;
	for (int i = 0; i < arrinfo_p->nchunks; i++) 
	    if (local_id == arrinfo_p->chunks[i].node_id)
		count++;
	indexinfo.chunk_indexs.assign(count, chunk_index);

	pool_init(nthreads);
	for (int i = 0; i < arrinfo_p->nchunks; i++) 
	    if (local_id == arrinfo_p->chunks[i].node_id) {
		bufsize = (arrinfo_p->compress_type == CompressType::NO_COMPRESSION) ? arrinfo_p->chunks[i].chunk_size : arrinfo_p->chunks[i].compressed_size;
		buf = (char *)malloc(bufsize);
		assert(buf);
		string path = arrinfo_p->name + "/" + arrinfo_p->name + "_" + lexical_cast<string>(i);
		get_disk_chunk_data(buf, bufsize, path.c_str());

		indexinfo.chunk_indexs[pos].chunk_id = i;
		// add job to thread pool
		targ = (genindex_arg_t *)malloc(sizeof(filterdata_arg_t));
		assert(targ);
		targ->compress_type = arrinfo_p->compress_type;
		targ->buf = buf;
		targ->chunkinfo = &(arrinfo_p->chunks[i]);
		targ->chunk_index = &(indexinfo.chunk_indexs[pos]);
		targ->dtype_size = arrinfo_p->dtype_size;
		targ->dtype = arrinfo_p->var_type;
		targ->fill_value = &(arrinfo_p->fill_value);
		pool_add_job(gen_chunk_index, targ);
		pos++;
	    }
	pool_destroy();
    }
    else {
	for (int i = 0; i < arrinfo_p->nchunks; i++) 
	    if (local_id == arrinfo_p->chunks[i].node_id) 
		create_chunk_indexinfo(indexinfo, arrinfo_p->chunks[i], arrinfo_p->var_type, arrinfo_p->fill_value);
    }
   
    /* debug */
    _return = "";
    double min, max;
    min = indexinfo.chunk_indexs[0].min;
    max = indexinfo.chunk_indexs[0].max;
    for (size_t i = 0; i < indexinfo.chunk_indexs.size(); i++) {
	if (min > indexinfo.chunk_indexs[i].min)
	    min = indexinfo.chunk_indexs[i].min;
	if (max < indexinfo.chunk_indexs[i].max)
	    max = indexinfo.chunk_indexs[i].max;

	_return +=  lexical_cast<string>(indexinfo.chunk_indexs[i].chunk_id) + " " + 
	    lexical_cast<string>(indexinfo.chunk_indexs[i].min) + " " +
	    lexical_cast<string>(indexinfo.chunk_indexs[i].max) + "\n";
    }

    _return += "in sumary:\nmin: " + lexical_cast<string>(min) + 
		"\nmax: " + lexical_cast<string>(max) + "\n";
    
    /* write to disk */
    write_indexinfo(indexinfo);

    // remove the previous array info with the same name
    for (size_t i = 0; i < index_infos.size(); i++) {
	if (array_name == index_infos[i].array_name)
	    index_infos.erase(index_infos.begin()+i);
    }
    index_infos.push_back(indexinfo);
}

int ArrayDBHandler::count_chunk_id_by_chunk_index(const ArrayInfo& arrinfo, std::vector<int64_t>& first_chunk, std::vector<int64_t>& chunk_lens, int index) {
    int chunk_id, i, j, pos;
    std::vector<int64_t> chunk_coords(arrinfo.ndims, 0);

    /* count chunk coords */
    pos = index;
    for (i = 0, j = arrinfo.ndims - 1; i < arrinfo.ndims; i++, j--) {
	chunk_coords[j] = first_chunk[j] + pos % chunk_lens[j];
	pos = pos / chunk_lens[j];
    }

    /* count chunk id */
    chunk_id = count_chunk_id_by_chunk_coords(arrinfo, chunk_coords);
    return chunk_id;
}

inline int ArrayDBHandler::count_chunk_id_by_chunk_coords(const ArrayInfo& arrinfo, std::vector<int64_t>& chunk_coords) {
    int chunk_id, i;
    chunk_id = chunk_coords[0];
    for (i = 1; i < arrinfo.ndims; i++)
	chunk_id = chunk_id * arrinfo.chunk_dims[i] + chunk_coords[i];

    return chunk_id;
}


void ArrayDBHandler::get_subarray_chunkids(std::vector<int>& chunk_ids, const ArrayInfo& arrinfo, const std::vector<int64_t>& first, const std::vector<int64_t>& last) {
    std::vector<int64_t> first_chunk(arrinfo.ndims, 0);
    std::vector<int64_t> last_chunk(arrinfo.ndims, 0);
    std::vector<int64_t> chunk_lens(arrinfo.ndims, 0);

    int i, chunk_id;
    int64_t nchunks;

    /* count chunk coords */
    for (i = 0; i < arrinfo.ndims; i++) {
	first_chunk[i] = first[i] / arrinfo.dims[i].chunk_length;
	last_chunk[i] = last[i] / arrinfo.dims[i].chunk_length;
	chunk_lens[i] = last_chunk[i] - first_chunk[i] + 1;
    }

    /* count chunk num */
    nchunks = chunk_lens[0];
    for (i = 1; i < arrinfo.ndims; i++) {
	nchunks = nchunks * chunk_lens[i];
    }

    /* count chunk id */
    chunk_ids.clear();
    for (i = 0; i < nchunks; i++) {
	chunk_id = count_chunk_id_by_chunk_index(arrinfo, first_chunk, chunk_lens, i);
	chunk_ids.push_back(chunk_id);
    }
}

void ArrayDBHandler::executeQuery(std::string& _return, const std::string& request, const std::string& data) {
    int64_t i;
    ArrayInfo *arrinfo_p;
    string query(request);
    ArrayDBError status;

    if (local_role != MASTER) {
	_return = "error: the server is not a master";
	printf("error: get a message from client, but I'm not a master\n");
	return;
    }

    /* create sentence */
    if (istarts_with(query, "create")) {
	erase_first(query, "create");
	trim(query);
	if (istarts_with(query, "array")) {
	    parse_create_array(query, _return);
	} /* end if (istarts_with(query, "array") */
	if (istarts_with(query, "index")) {
	    parse_create_index(query, _return);
	}
    }/* end if (istarts_with(query, "create"); */
    else if (istarts_with(query, "store")) {
	std::vector<string> strs;
	string name;
	CompressType::type compress_type;
	boost::split(strs, query, is_any_of(" \t"), token_compress_on);
	name = strs[1];
	compress_type = CompressType::NO_COMPRESSION;
	if (strs.size() >= 4) {
	    if (strs[3] == "lzma")
		compress_type = CompressType::LZMA;
	    else if (strs[3] == "zlib")
		compress_type = CompressType::ZLIB;
	}
	    
	status = distribute_array(_return, name, data, compress_type);
	if (status != SUCCESS) {
	    _return = arraydb_error(status); 
	}
    }
    else if (istarts_with(query, "show")) {
	std::vector<string> strs;
	boost::split(strs, query, is_any_of(" \t"), token_compress_on);

	arrinfo_p = get_arrinfo(strs[1]);
	if (arrinfo_p == NULL) {
	    _return = arraydb_error(ARRAY_NOT_EXIST);
	}
	else {
	    _return = arrinfo_p->name + " <" 
		+ arrinfo_p->var_name + ": " 
		+ get_type_str(arrinfo_p->var_type) + "> [";
	    for (i = 0; i < arrinfo_p->ndims; i++) {
		_return += arrinfo_p->dims[i].name + " = "
		    + lexical_cast<string>(arrinfo_p->dims[i].length) + ", "
		    + lexical_cast<string>(arrinfo_p->dims[i].chunk_length) + ", "
		    + lexical_cast<string>(arrinfo_p->dims[i].overlap_length) + "; ";
	    }
	    _return += "]\n";
	    _return += "fill_value: " + arrinfo_p->fill_value;
	    _return += "\ncompress type: " + string((CompressorFactory::getInstance().getCompressors())[arrinfo_p->compress_type]->getName());
	    print_array_info(*arrinfo_p);
	}
    }
    else if (istarts_with(query, "subarray")) {
	string name;
	std::vector<int64_t> first;
	std::vector<int64_t> last;

	/* parse subarray */
	status = parse_subarray(query, name, first, last);
	if (status != SUCCESS) {
	    _return = arraydb_error(status);
	}

	status = check_overflow(name, first, last);
	if (status == SUCCESS) {
	    /* get data from nodes */
	    status = execute_subarray(_return, name, first, last);
	}
	else {
	    _return = arraydb_error(status);
	}
    }
    else if (istarts_with(query, "select")) {
	parse_select(_return, query);
    }
    else if (istarts_with(query, "set")) {
	parse_set_clause(_return, query);
    }
    else if (istarts_with(query, "get")) {
	//parse_get_clause(_return, query);
    }
    printf("\nexecuteQuery: %s\n", request.data());
} /* end executeQuery */

void ArrayDBHandler::echo(std::string& _return, const std::string& request) {
    // Your implementation goes here
    /* master part */
    if (local_role == MASTER) {
	size_t i;
	string res;
	_return = "";
	for (i = 0; i < clients.size(); i++) {
	    clients[i].get_client()->echo(res, request);
	    _return = _return + clients[i].get_address() + "-> " + res + "\n";
	}
	cout<<_return<<endl;
    }

    /* worker part */
    else {
	_return = "Hello, " + request;
	std::cout<<_return<<endl;
    }
}


void ArrayDBHandler::store(std::string& _return, const Array& chunk, const std::string& array_name) {
    
    int chunk_id;
    ArrayInfo *arrinfo_p;

    chunk_id = lexical_cast<int>(chunk.name); 
    arrinfo_p = get_arrinfo(array_name);
    assert(arrinfo_p);

    put_chunk_data(chunk.data.c_str(), arrinfo_p->chunks[chunk_id], chunk.compress_type);
   
    _return = lexical_cast<string>(arrinfo_p->chunks[chunk_id].compressed_size);
}

void ArrayDBHandler::create_arrinfo(const ArrayInfo& arrinfo) {
    
    // remove the previous array info with the same name
    for (size_t i = 0; i < array_infos.size(); i++) {
	if (arrinfo.name == array_infos[i].name)
	    array_infos.erase(array_infos.begin()+i);
    }
    
    // Your implementation goes here
    write_arrinfo(arrinfo);
    
    // remove the previous array info with the same name
    for (size_t i = 0; i < array_infos.size(); i++) {
	if (arrinfo.name == array_infos[i].name)
	    array_infos.erase(array_infos.begin()+i);
    }
    // create data dir
    string cmd = "mkdir ./" + arrinfo.name;
    int ret; 
    ret = system(cmd.data());
    printf("create_arrinfo\n");
}

void ArrayDBHandler::load(Array& _return, const std::string& array_name) {
    // Your implementation goes here
    printf("load\n");
}

/* master call this to let worker load chunk */
void ArrayDBHandler::load_chunk(std::string& _return, const std::string& name, const int32_t id, const std::vector<int64_t> & first, const std::vector<int64_t> & last) {
    // Your implementation goes here
    FILE *fh;
    size_t bufsize, bytes_read;
    int64_t i, j, is_all;
    string fname;
    char *buf;
    ArrayInfo *arrinfo_p;
    std::vector<int64_t> chunk_local_first(first.size(), 0);
    std::vector<int64_t> chunk_local_last(first.size(), 0);

    /* read data from file */
    fname = "./" + name + "/" + name + "_" + lexical_cast<string>(id);
    fh = fopen(fname.data(), "rb");
    assert(fh != NULL);
    bufsize = get_file_size(fh);

    buf = (char *)malloc(bufsize + 1);
    assert(buf != NULL);
    bytes_read = fread(buf, 1, bufsize, fh);
    assert(bytes_read == bufsize);
    fclose(fh);

    /* count chunk local coordinators */
    arrinfo_p = get_arrinfo(name);
    //printf("name %s\n", name.data());
    assert(arrinfo_p != NULL);
    is_all = 1;
    for (j = 0; j < arrinfo_p->ndims; j++) {
	if (arrinfo_p->chunks[id].first[j] < first[j]) {
	    chunk_local_first[j] = first[j] - arrinfo_p->chunks[id].first[j];
	    is_all = 0;
	}
	else
	    chunk_local_first[j] = 0;
	if (arrinfo_p->chunks[id].last[j] > last[j]) {
	    chunk_local_last[j] = last[j] - arrinfo_p->chunks[id].first[j];
	    is_all = 0;
	}
	else
	    chunk_local_last[j] = arrinfo_p->chunks[id].last[j] - arrinfo_p->chunks[id].first[j];
    }

    if (0 == is_all) {
	int64_t nelmts, copy_num, copy_len, chunk_stride;
	char *tbuf;
	size_t tbuf_pos, buf_pos;
	nelmts = 1;
	for (i = 0; i < arrinfo_p->ndims; i++)
	    nelmts *= (chunk_local_last[i] - chunk_local_first[i] + 1);

	bufsize = nelmts * arrinfo_p->dtype_size;
	tbuf = (char *)malloc(bufsize);
	assert(tbuf != NULL);
	tbuf_pos = 0;

	/*  2d array */
	if (2 == arrinfo_p->ndims) {
	    chunk_stride = arrinfo_p->chunks[id].last[1] - arrinfo_p->chunks[id].first[1] + 1;
	    copy_num = chunk_local_last[1] - chunk_local_first[1] + 1;
	    buf_pos = (chunk_local_first[0] * chunk_stride + chunk_local_first[1]) * arrinfo_p->dtype_size;
	    for (i = chunk_local_first[0]; i <= chunk_local_last[0]; i++) {
		copy_len = copy_num * arrinfo_p->dtype_size;
		memcpy(&(tbuf[tbuf_pos]), &(buf[buf_pos]), copy_len);
		buf_pos += (chunk_stride * arrinfo_p->dtype_size);
		tbuf_pos += copy_len;
	    }
	}

	/* TODO: 3d array */
	else if (3 == arrinfo_p->ndims) {
	    assert(0);
	}
	free(buf);
	buf = tbuf;
	tbuf = NULL;
    }

    _return.assign(buf, bufsize);
    free(buf);
    printf("load_chunk\n");
}



/* execute subarray */
ArrayDBError ArrayDBHandler::execute_subarray(string& data, const string& name, vector<int64_t>& first, vector<int64_t>& last) {
    std::vector<int> chunk_ids;
    const ArrayInfo *arrinfo_p;

    arrinfo_p = get_arrinfo(name);
    assert(arrinfo_p != NULL);

    get_subarray_chunkids(chunk_ids, *arrinfo_p, first, last);

    size_t i;
    for (i = 0; i < chunk_ids.size(); i++) {
	cout<<chunk_ids[i]<<", ";
    }
    cout<<endl;

    gather_chunk_data(data, *arrinfo_p, chunk_ids, first, last);

    return SUCCESS;
}

int ArrayDBHandler::get_index_by_chunk_id(const std::vector<int>& chunk_ids, int chunk_id) {
    size_t i;

    for (i = 0; i < chunk_ids.size(); i++) {
	if (chunk_id == chunk_ids[i])
	    return i;
    }
    return -1;
}

void ArrayDBHandler::gather_chunk_data(string &data, const ArrayInfo& arrinfo, const std::vector<int>& chunk_ids, const std::vector<int64_t>& first, const std::vector<int64_t>& last) {
    int node_id, chunk_id, chunk_index;
    char *buf;
    std::vector<string> chunk_data(chunk_ids.size(), "");
    std::vector<int64_t> chunk_data_pos(chunk_ids.size(), 0);

    /* get chunk data from all nodes */
    for (size_t i = 0; i < chunk_ids.size(); i++) {
	node_id = get_node_id_by_chunk_id(arrinfo, chunk_ids[i]);
	get_node_handler(node_id)->get_client()->
	    load_chunk(chunk_data[i], arrinfo.name, chunk_ids[i], first, last);
    }

    /* assemble chunks */
    int64_t nelmts, copy_len, copy_num, buf_pos, i, j;
    std::vector<int64_t> chunk_2d_coords(2, 0);
    std::vector<int64_t> chunk_3d_coords(3, 0);

    nelmts = 1;
    for (i = 0; i < arrinfo.ndims; i++) 
	nelmts *= (last[i] - first[i] + 1);

    buf = (char *)malloc(arrinfo.dtype_size * nelmts);
    assert(buf != NULL);
    buf_pos = 0;

    /* 2D array */
    if (2 == arrinfo.ndims) {
	for (i = first[0]; i <= last[0]; i++)
	    for (j = first[1]; j <= last[1]; j += copy_num) {
		chunk_2d_coords[0] = i / arrinfo.dims[0].chunk_length;
		chunk_2d_coords[1] = j / arrinfo.dims[1].chunk_length;
		chunk_id = count_chunk_id_by_chunk_coords(arrinfo, chunk_2d_coords);
		chunk_index = get_index_by_chunk_id(chunk_ids, chunk_id);
		if (last[1] > arrinfo.chunks[chunk_id].last[1])
		    copy_num = arrinfo.chunks[chunk_id].last[1] - j + 1;
		else
		    copy_num = last[1] - j + 1;
		copy_len = copy_num * arrinfo.dtype_size;

		memcpy(&(buf[buf_pos]), &(chunk_data[chunk_index][chunk_data_pos[chunk_index]]), copy_len);
		buf_pos += copy_len;
		chunk_data_pos[chunk_index] += copy_len;
	    }
    }
    /* TODO:3D array */
    else if (3 == arrinfo.ndims) {
	assert(0);
    }

    bin_to_string(data, buf, arrinfo.var_type, nelmts);
    free(buf);
}

/* parse subarray */
ArrayDBError ArrayDBHandler::parse_subarray(const string& query, string& name, std::vector<int64_t>& first, std::vector<int64_t>& last) {
    string str;
    std::vector<string> strs;
    int i, pos;
    int64_t value;
    ArrayInfo *arrinfo_p;

    pos = query.find_first_of("(");
    str = query.substr(pos, query.size() - pos);
    trim_if(str, is_any_of("() \t"));
    boost::split(strs, str, is_any_of(","), token_compress_on);

    name = trim_copy_if(strs[0], is_any_of(" \t"));

    arrinfo_p = get_arrinfo(name);
    if (arrinfo_p == NULL) 
	return ARRAY_NOT_EXIST;

    if ((arrinfo_p->ndims * 2 + 1) != (int64_t)strs.size())
	return SUBARRAY_PARAMS_ERROR;

    for (i = 1; i <= arrinfo_p->ndims; i++) {
	value = lexical_cast<int64_t>(trim_copy_if(strs[i], is_any_of(" \t")));
	first.push_back(value);
	value = lexical_cast<int64_t>(trim_copy_if(strs[i + arrinfo_p->ndims], is_any_of(" \t")));
	last.push_back(value);
    }

    return SUCCESS;
}


int ArrayDBHandler::get_node_id_by_chunk_id(const ArrayInfo& arrinfo, int chunk_id) {
    return arrinfo.chunks[chunk_id].node_id;
}

void ArrayDBHandler::map_chunk(ArrayInfo& arrinfo, StorageStrategy::type ss) {
    switch(ss) {
	case StorageStrategy::ROUND_ROBIN:
	    map_chunk_round_robin(arrinfo);
	    break;
	default:
	    printf("strategy %d: not implementation yet\n", ss);
	    break;
    }
}

/* use round robin to map array chunks into nodes */
void ArrayDBHandler::map_chunk_round_robin(ArrayInfo& arrinfo) {
    int64_t i;
    int j;
    size_t chunk_dim;

    arrinfo.ss = StorageStrategy::ROUND_ROBIN;

    /* chunk number */
    arrinfo.nchunks = 1;
    for (i = 0; i < arrinfo.ndims; i++) {
	if ((arrinfo.dims[i].length % arrinfo.dims[i].chunk_length) != 0)
	    chunk_dim = arrinfo.dims[i].length / arrinfo.dims[i].chunk_length + 1;
	else     
	    chunk_dim = arrinfo.dims[i].length / arrinfo.dims[i].chunk_length;
	arrinfo.nchunks *= chunk_dim;
	arrinfo.chunk_dims.push_back(chunk_dim);
    }

    /* chunk info */
    std::vector<int64_t> first(arrinfo.ndims, 0);
    std::vector<int64_t> last(arrinfo.ndims, 0);
    for (i = 0; i < arrinfo.ndims; i ++) {
	last[i] = arrinfo.dims[i].chunk_length - 1;
    }

    for (i = 0; i < arrinfo.nchunks; i++) {
	ChunkInfo chunk_info;
	chunk_info.chunk_id = i;
	chunk_info.node_id = clients[i % clients.size()].get_nodeid();  
	chunk_info.ndims = arrinfo.ndims;
	chunk_info.compressed_size = 0;
	chunk_info.chunk_name = arrinfo.name;
	chunk_info.first = first;
	chunk_info.last = last;

	chunk_info.chunk_size = arrinfo.dtype_size;
	for (j = 0; j < chunk_info.ndims; j++) {
	    chunk_info.chunk_size *= (last[j] - first[j] + 1);
	}

	for (j = chunk_info.ndims - 1; j >= 0; j--) {
	    first[j] += arrinfo.dims[j].chunk_length;
	    last[j] += arrinfo.dims[j].chunk_length;
	    if (last[j] >= arrinfo.dims[j].length)
		last[j] = arrinfo.dims[j].length - 1;
	    if (first[j] >= arrinfo.dims[j].length) {
		first[j] = 0;
		last[j] = arrinfo.dims[j].chunk_length - 1;
	    }
	    else
		break;
	}
	arrinfo.chunks.push_back(chunk_info);
    }
}

ArrayDBError ArrayDBHandler::distribute_array(string& _return, const string& name, const string& data, CompressType::type compress_type) {
    ArrayInfo *arrinfo_p;
    char **chunk_buf;
    int64_t i, nelmts;
    size_t copy_len;
    size_t *chunk_buf_pos;

    arrinfo_p = get_arrinfo(name);
    if (arrinfo_p == NULL)
	return ARRAY_NOT_EXIST;

    arrinfo_p->compress_type = compress_type;

    /* allocate storage space for chunks */
    chunk_buf_pos = new size_t[arrinfo_p->nchunks];
    chunk_buf = new char *[arrinfo_p->nchunks];
    for (i = 0; i < arrinfo_p->nchunks; i++) {
	chunk_buf[i] = new char[arrinfo_p->chunks[i].chunk_size];
	chunk_buf_pos[i] = 0;
    }

    nelmts = arrinfo_p->nelmts;
    /* copy data into chunks */
    size_t chunk_id;
    size_t copy_num;
    for (i = 0; i < nelmts; i += copy_num) {
	chunk_id = count_chunk_id_by_elmt_index(arrinfo_p, i);
	/* count copy_len */
	if ((i % arrinfo_p->dims[arrinfo_p->ndims - 1].length) == ((arrinfo_p->chunk_dims[arrinfo_p->ndims - 1] - 1) * arrinfo_p->dims[arrinfo_p->ndims - 1].chunk_length)) 
	    copy_num = arrinfo_p->dims[arrinfo_p->ndims - 1].length - (arrinfo_p->chunk_dims[arrinfo_p->ndims - 1] - 1) * arrinfo_p->dims[arrinfo_p->ndims - 1].chunk_length;
	else
	    copy_num = arrinfo_p->dims[arrinfo_p->ndims - 1].chunk_length;
	copy_len = copy_num * arrinfo_p->dtype_size;
	/*cout<<"i:        "<<i<<endl;
	cout<<"copy_num: "<<copy_num<<endl;
	cout<<"copy_len: "<<copy_len<<endl;
	cout<<"chunk_id: "<<chunk_id<<endl;*/
	memcpy(&(chunk_buf[chunk_id][chunk_buf_pos[chunk_id]]), &((data.data())[i * arrinfo_p->dtype_size]), copy_len);
	chunk_buf_pos[chunk_id] += copy_len;
    }

    /* distribute chunks into according nodes */
    /* TODO: string copy */
    string reply;
    Array arr;
    _return = "";
    for (i = 0; i < arrinfo_p->nchunks; i++) {
	arr.name = lexical_cast<std::string>(i);
	arr.data.assign(chunk_buf[i], chunk_buf_pos[i]);
	arr.compress_type = compress_type;
	get_node_handler(arrinfo_p->chunks[i].node_id)->get_client()->store(reply, arr, arrinfo_p->name);	
	//_return += get_node_handler(arrinfo_p->chunks[i].node_id)->get_address() + "->" + reply + "\n";
	arrinfo_p->chunks[i].compressed_size = lexical_cast<int64_t>(reply);
    }

    // update array info
    write_arrinfo(*arrinfo_p);

    // populate the array info to other nodes
    for (size_t i = 0; i < clients.size(); i++) {
	clients[i].get_client()->create_arrinfo(*arrinfo_p);
    }
    
    /* free */
    for (i = 0; i < arrinfo_p->nchunks; i++) {
	delete []chunk_buf[i];
    }
    delete []chunk_buf;
    delete []chunk_buf_pos;
    return SUCCESS;
}

/* get node handler according to node id */
ServerConnection * ArrayDBHandler::get_node_handler(int node_id) {
    return &(clients[node_id - 1]);
}

size_t ArrayDBHandler::count_chunk_id_by_elmt_index(const ArrayInfo *arrinfo_p, size_t elmt_index) {
    std::vector<int64_t> chunk_coords(arrinfo_p->ndims, 0);
    int64_t i, j;
    int64_t pos, chunk_id, coord;

    /* count chunk_coords */
    pos = elmt_index;
    chunk_id = 0;
    for (i = 0, j = arrinfo_p->ndims - 1; i < arrinfo_p->ndims; i++, j--) {
	coord = pos % arrinfo_p->dims[j].length;
	pos = pos / arrinfo_p->dims[j].length;
	chunk_coords[j] = coord / arrinfo_p->dims[j].chunk_length;
    }

    /* count chunk_id */
    /*chunk_id = chunk_coords[0];
      for (i = 1; i < arrinfo_p->ndims; i++) {
      chunk_id = chunk_id * arrinfo_p->chunk_dims[i] + chunk_coords[i];
      }*/
    chunk_id = count_chunk_id_by_chunk_coords(*arrinfo_p, chunk_coords);

    return chunk_id;
}

ArrayInfo* ArrayDBHandler::get_arrinfo(const string& arrname) {
    size_t i;

    /* search from memory */
    for (i = 0; i < array_infos.size(); i++) {
	if (arrname == array_infos[i].name)
	    return &(array_infos[i]);
    }

    /* search from disk */
    FILE *fh;
    string fname, arr_meta;
    size_t fsize;
    char *buf;
    ArrayInfo *arrinfo_p;
    ArrayInfo arrinfo;

    arrinfo_p = NULL;
    fname = arrname + ".meta";
    fh = fopen(fname.data(), "rb");
    if (fh != NULL) {
	size_t read_size;
	fsize = get_file_size(fh);
	buf = new char[fsize + 1];
	read_size = fread(buf, 1, fsize, fh);
	assert(read_size == fsize);
	fclose(fh);

	arr_meta.assign(buf, fsize);
	deserialize(arrinfo, arr_meta);

	array_infos.push_back(arrinfo);
	arrinfo_p = &(array_infos[array_infos.size() - 1]);

	delete buf;
	buf = NULL;
    }

    return arrinfo_p;
}

void ArrayDBHandler::write_arrinfo(const ArrayInfo& arrinfo) {
    string arr_str;
    string fname;
    FILE *fh;
    serialize<ArrayInfo>(arr_str, arrinfo);
    fname = arrinfo.name + ".meta";
    fh = fopen(fname.data(), "wb");
    assert(fh != NULL);
    fwrite(arr_str.data(), 1, arr_str.size(), fh);
    fclose(fh);
}

ArrayDBError ArrayDBHandler::check_overflow(const string& name, std::vector<int64_t>& first, std::vector<int64_t>& last) {
    ArrayInfo *arrinfo_p;
    int i;

    arrinfo_p = get_arrinfo(name);
    if (arrinfo_p == NULL)
	return ARRAY_NOT_EXIST;

    for (i = 0; i < arrinfo_p->ndims; i++) {
	if (first[i] < 0 || last[i] >= arrinfo_p->dims[i].length)
	    return COORDS_OVERFLOW;
    }

    return SUCCESS;
}

void ArrayDBHandler::print_array_info(const ArrayInfo& arrinfo) {
    int64_t i, j;

    /* debug print */
    cout<<"array id: "<<arrinfo.array_id<<"\nchunk num: "<<arrinfo.nchunks
	<<"\nstorage strategy: "<<arrinfo.ss<<"\n dtype_size: "<<arrinfo.dtype_size
	<<"\nnelmts: "<<arrinfo.nelmts
	<<"\narray_name: "<<arrinfo.name<<"\nvar_name: "<<arrinfo.var_name
	<<"\nvar_type: "<<arrinfo.var_type<<"\nndims: "<<arrinfo.ndims;
    /* dim info */
    for (i = 0; i < arrinfo.ndims; i++) {
	cout<<"\n\t dim name: "<<arrinfo.dims[i].name
	    <<"\n\t dim len: "<<arrinfo.dims[i].length
	    <<"\n\t dim chunk len: "<<arrinfo.dims[i].chunk_length
	    <<"\n\t dim overlap len: "<<arrinfo.dims[i].overlap_length
	    <<"\n\t ----------------------------------";
    }
    /* chunk dims */
    cout<<"\n\nchunk dims: \n";
    for (i = 0; i < arrinfo.ndims; i++) {
	cout<<"\t"<<arrinfo.chunk_dims[i];
    }
    /* chunk info */
    for (i = 0; i < arrinfo.nchunks; i++) {
	cout<<"\n\t chunk id: "<<arrinfo.chunks[i].chunk_id
	    <<"\n\t node id: "<<arrinfo.chunks[i].node_id
	    <<"\n\t chunk size: "<<arrinfo.chunks[i].chunk_size;
	cout<<"\n\t first: ("; 
	for (j = 0; j < arrinfo.ndims; j++) {
	    cout<<","<<arrinfo.chunks[i].first[j];
	}
	cout<<")\n\t last:   ("; 
	for (j = 0; j < arrinfo.ndims; j++) {
	    cout<<","<<arrinfo.chunks[i].last[j];
	}
	cout<<")\n\t ---------------------------------"<<endl;
    }

}

void ArrayDBHandler::parse_create_array(string& query, string& _return) {
    ArrayInfo arrinfo;
    //ArrayInfo *arrinfo_p;
    DimInfo diminfo;
    std::vector<string> strs;
    int pos;
    int64_t i;

    /* TODO: array id not a good idea */
    arrinfo.array_id = array_infos.size();
    erase_first(query, "array");
    trim(query);
    /* array name */
    boost::split(strs, query, is_any_of("<"), token_compress_off);
    arrinfo.name = trim_copy(strs[0]);

    /* variable name */
    query = trim_copy(strs[1]);
    split(strs, query, is_any_of(":"), token_compress_off);
    arrinfo.var_name = trim_copy(strs[0]);
    /* variable type and size */
    pos = query.find_first_of(':');
    query = query.substr(pos + 1, query.size() - pos - 1);
    split(strs, query, is_any_of(">"), token_compress_off);
    arrinfo.var_type = get_datatype(trim_copy(strs[0]), &(arrinfo.dtype_size));
    /* dimension num */
    query = trim_copy_if(strs[1], is_any_of(" []"));
    split(strs, query, is_any_of("="), token_compress_off);
    arrinfo.ndims = strs.size() - 1;
    /* dimension name and length */
    arrinfo.nelmts = 1;
    for (i = 0; i < arrinfo.ndims; i++) {	
	diminfo.name = trim_copy(strs[0]);
	pos = query.find_first_of(':');
	query = query.substr(pos + 1, query.size() - pos - 1);
	strs = split(strs, query, is_any_of(","), token_compress_off);
	diminfo.length = lexical_cast<size_t>(trim_copy(strs[0]));
	diminfo.chunk_length = lexical_cast<size_t>(trim_copy(strs[1]));
	diminfo.overlap_length = lexical_cast<size_t>(trim_copy(strs[2]));
	if (i < (arrinfo.ndims - 1)) {
	    pos = query.find_first_of(',');
	    query = query.substr(pos + 1, query.size() - pos - 1);
	    pos = query.find_first_of(',');
	    query = query.substr(pos + 1, query.size() - pos - 1);
	    pos = query.find_first_of(',');
	    query = query.substr(pos + 1, query.size() - pos - 1);
	    split(strs, query, is_any_of("="), token_compress_off);
	}
	arrinfo.nelmts *= diminfo.length;
	arrinfo.dims.push_back(diminfo);
    }

    arrinfo.fill_value = "0";
    arrinfo.compress_type = CompressType::NO_COMPRESSION;
    /* map chunk to nodes */
    map_chunk(arrinfo, StorageStrategy::ROUND_ROBIN);

    // remove the previous array info with the same name
    for (size_t i = 0; i < array_infos.size(); i++) {
	if (arrinfo.name == array_infos[i].name)
	    array_infos.erase(array_infos.begin()+i);
    }

    /* store array info into global variables */
    array_infos.push_back(arrinfo);

    /* wirte array info into disk */
    write_arrinfo(arrinfo);

    /* populate the array info to other nodes */
    for (size_t i = 0; i < clients.size(); i++) {
	clients[i].get_client()->create_arrinfo(arrinfo);
    }

    //print_array_info(arrinfo);

}

void ArrayDBHandler::parse_create_index(string& query, string& _return) {
    
    ArrayInfo *arrinfo_p;
    DimInfo diminfo;
    std::vector<string> strs;

    trim(query);
    
    /* array name */
    boost::split(strs, query, is_any_of(" \t"), token_compress_on);
    if (strs.size() < 5) {
	_return = "Usage: create index [index name] [index type] on [array name]\n";
	return;
    }

    /* index name: strs[1] */
    /* index type: strs[2] */
    /* array name: strs[4] */
    arrinfo_p = get_arrinfo(strs[4]);
    if (arrinfo_p == NULL) {
	_return = "array name not exist!";
	return;
    }

    IndexType::type index_type;
    if (strs[2] == "chunk_index")
	index_type = IndexType::CHUNK_INDEX;
    else if (strs[2] == "bitmap_index")
	index_type = IndexType::BITMAP_INDEX;
    else if (strs[2] == "tile_index")
	index_type = IndexType::TILE_INDEX;
    else if (strs[2] == "chunk_bitmap")
	index_type = IndexType::CHUNK_BITMAP;
    else if (strs[2] == "chunk_tile")
	index_type = IndexType::CHUNK_TILE;
    else {
	_return = "index type is no_index\n";
	return;
    }

    _return = "";
    for (size_t i = 0; i < clients.size(); i++) { 
	string ret;
	clients[i].get_client()->create_indexinfo(ret, strs[4], strs[1], index_type);
	_return += ret;
    }

}


void ArrayDBHandler::create_chunk_indexinfo(IndexInfo& indexinfo, ChunkInfo& chunkinfo, DataType::type dtype, string& fill_value) {

    ChunkIndex chunk_index;
    char *buf;
    ArrayInfo *arrinfo_p;

    arrinfo_p = get_arrinfo(indexinfo.array_name);

    /* read chunk data from disk */
    buf = (char *)malloc(chunkinfo.chunk_size);
    assert(buf != NULL);
    get_chunk_data(buf, chunkinfo, arrinfo_p->compress_type);
    
    /* find min and max */
    double max, min;
    int64_t nelmts = chunkinfo.chunk_size / 4;
    get_chunk_data_range(min, max, buf, nelmts, dtype, fill_value);
    
    chunk_index.chunk_id = chunkinfo.chunk_id;
    chunk_index.max = max;
    chunk_index.min = min;

    indexinfo.chunk_indexs.push_back(chunk_index);
}

void ArrayDBHandler::write_indexinfo(const IndexInfo& indexinfo) {
    string index_str;
    string fname;
    FILE *fh;
    serialize<IndexInfo>(index_str, indexinfo);
    fname = indexinfo.array_name + ".chunk_index";
    fh = fopen(fname.data(), "wb");
    assert(fh != NULL);
    fwrite(index_str.data(), 1, index_str.size(), fh);
    fclose(fh);
}


void ArrayDBHandler::parse_select(string& _return, string& _query) {

    ArrayInfo *arrinfo_p;
    std::vector<string> strs;
    string var_name, min, max;
    string query(_query);

    boost::split(strs, query, is_any_of(" \t"), token_compress_on);
    
    /* select variable_name from array_name where () and () 
     * strs[1]: var_name
     * strs[3]: arr_name
     */

    var_name = strs[1];

    // find array
    arrinfo_p = get_arrinfo(strs[3]);
    if (arrinfo_p == NULL) {
	_return = "array name not exist!";
	return;
    }

    
    // set min and max
    int pos = query.find("where");
    if (pos < 0) {
	//TODO: subarray all
    }
    else {
	query = query.substr(pos + 5, query.size() - pos - 5);
	trim(query);
	
	// suppose the where clause is only two type: with "and" or without
	pos = query.find("and");
	if (pos < 0) {
	    set_min_max(min, max, query);
	}
	// with an "and"
	else {
	    string q1, q2;
	    string min1, max1, min2, max2;

	    q1 = query.substr(0, pos);
	    trim(q1);
	    set_min_max(min1, max1, q1);

	    q2 = query.substr(pos + 3, query.size() - pos - 3);
	    trim(q2);
	    set_min_max(min2, max2, q2);

	    if (min1 > min2)
		min = min1;
	    else
		min = min2;

	    if (max1 > max2)
		max = max2;
	    else
		max = max1;
	}
    }
    
    // debug
    cout<<"min: "<<min<<"\nmax: "<<max<<endl;

    _return = "";
    for (size_t i = 0; i < clients.size(); i++) { 
	string ret;
	clients[i].get_client()->filter(ret, arrinfo_p->name, var_name, min, max);
	_return += ret;
    }  

}

void ArrayDBHandler::set_min_max(string& min, string& max, string& query) {
    int pos;
    std::vector<string> strs;
    
    if ((pos = query.find("<")) > 0) {
	boost::split(strs, query, is_any_of("< \t"), token_compress_on);
	max = strs[1];
	min = lexical_cast<string>(LONG_MIN); 
    }
    else if ((pos = query.find(">")) > 0) {
	boost::split(strs, query, is_any_of("> \t"), token_compress_on);
	max = lexical_cast<string>(LONG_MAX);
	min = strs[1]; 
    }
    else if ((pos = query.find("=")) > 0) {
	boost::split(strs, query, is_any_of("= \t"), token_compress_on);
	max = strs[1];
	min = strs[1]; 
    }
}

void ArrayDBHandler::parse_set_clause(string& _return, string& q) {
    string query(q);
    string ret;
    std::vector<string> strs;
      
    boost::split(strs, query, is_any_of(" \t"), token_compress_on);

    _return = "";
    if (strs.size() == 3) {
	set_para(ret, strs[1], strs[2]);
	for (size_t i = 0; i < clients.size(); i++) {
	    clients[i].get_client()->set_para(ret, strs[1], strs[2]);
	    _return += ret;
	}
    }
    else if (strs[1] == "fillvalue") {
	//TODO: change the arrayinfo	
	ArrayInfo *arrinfo_p;
	arrinfo_p = get_arrinfo(strs[2]);
	if (arrinfo_p == NULL){
	    _return = "no such array named " + strs[2] + "\n";
	    return;
	}
	arrinfo_p->fill_value = strs[3];
    	for (size_t i = 0; i < clients.size(); i++) {
	    clients[i].get_client()->set_fillvalue(ret, strs[2], strs[3]);
	    _return += ret;
	}
    }
    else
	_return = "Usage: set time/indextype/fillvalue [value]\n";

}

/*void ArrayDBHandler::parse_get_clause(string& _return, string& q) {
    string query(q);
    string ret;
    std::vector<string> strs;
      
    boost::split(strs, query, is_any_of(" \t"), token_compress_on);

    _return = "";
    if (strs[1] == "range_ratio") {
	ArrayInfo *arrinfo_p;
	ChunkInfo *chunkinfo_p;
	arrinfo_p = get_arrinfo(strs[3]);
	if (arrinfo_p == NULL) {
	    _return = "no such array\n";
	    return;
	}
	for (size_t i = 0; i < clients.size(); i++) {
	    clients[i].get_client()->set_time(ret, is_count_time);
	    _return += ret;
	}
    }
    else
	_return = "Usage: get range_ratio on array_name\n";

}*/
void ArrayDBHandler::filter(std::string& _return, const std::string& array_name, const std::string& var_name, const std::string& min_str, const std::string& max_str) {
    
    std::vector<chunk_type_info_t> chunk_typeinfos;
    double min, max;
    ArrayInfo *arrinfo_p;
    struct timeval start, end;
    chunk_type_info_t chunk_tinfo;

    // time start point
    gettimeofday(&start, NULL);

    arrinfo_p = get_arrinfo(array_name);
    assert(arrinfo_p != NULL);
    min = lexical_cast<double>(min_str);
    max = lexical_cast<double>(max_str);
    
    // set compare type, now only three kinds
    CompareType type;
    if (min == LONG_MIN)
	type = LESS;
    else if (max == LONG_MAX)
	type = LARGER;
    else
	type = LESS_LARGER;
    
    /*
     * set chunk ids which need to be filter
     */
    if (index_type == IndexType::NO_INDEX || index_type == IndexType::BITMAP_INDEX || index_type == IndexType::TILE_INDEX) {
	for (int i = 0; i < arrinfo_p->nchunks; i++) {
	    if (local_id == arrinfo_p->chunks[i].node_id) {
		chunk_tinfo.id = i;
		chunk_tinfo.type = CANDIDATE_CHUNK;
		chunk_typeinfos.push_back(chunk_tinfo);
	    }
	}
    }
    else {
	IndexInfo *indexinfo_p;
	// read index info from disk
	indexinfo_p = get_indexinfo(array_name);
	if (indexinfo_p == NULL) {
	    _return = "index not find\n";
	    return;
	}

	for (size_t i = 0; i < indexinfo_p->chunk_indexs.size(); i++) {
	    if (indexinfo_p->chunk_indexs[i].min >= min && indexinfo_p->chunk_indexs[i].max <= max) {
		chunk_tinfo.id = i;
		chunk_tinfo.type = VALID_CHUNK;
		chunk_typeinfos.push_back(chunk_tinfo);
	    }
	    else if (indexinfo_p->chunk_indexs[i].max >= min && indexinfo_p->chunk_indexs[i].min <= max) {
		chunk_tinfo.id = i;
		chunk_tinfo.type = CANDIDATE_CHUNK;
		chunk_typeinfos.push_back(chunk_tinfo);
	    }
	}
    }
    
    /*
     * filter chunk data
     */
    ArrayDBError status = UNKNOWN;
    if (arrinfo_p->var_type == DataType::FLOAT)
	status = filter_chunks<float>(_return, arrinfo_p, chunk_typeinfos, min, max, type);
    else if (arrinfo_p->var_type == DataType::DOUBLE)
	status = filter_chunks<double>(_return, arrinfo_p, chunk_typeinfos, min, max, type);
    else if (arrinfo_p->var_type == DataType::INT32)
	status = filter_chunks<int32_t>(_return, arrinfo_p, chunk_typeinfos, min, max, type);
    else if (arrinfo_p->var_type == DataType::INT64)
	status = filter_chunks<int64_t>(_return, arrinfo_p, chunk_typeinfos, min, max, type);
    
    if (status != SUCCESS) {
	_return = arraydb_error(status);
	return;
    }

    // time start point
    gettimeofday(&end, NULL);

    if (is_count_time) {
	double timeused;

	timeused = (end.tv_sec - start.tv_sec) + (double)(end.tv_usec - start.tv_usec) / 1000000;
	_return += "worker-" + lexical_cast<string>(local_id) + " used time: " +
	    lexical_cast<string>(timeused);
    }
}

IndexInfo* ArrayDBHandler::get_indexinfo(const string& arrname) {
    size_t i;

    /* search from memory */
    for (i = 0; i < index_infos.size(); i++) {
	if (arrname == index_infos[i].array_name)
	    return &(index_infos[i]);
    }

    /* search from disk */
    FILE *fh;
    string fname, index_meta;
    size_t fsize;
    char *buf;
    IndexInfo *indexinfo_p;
    IndexInfo indexinfo;

    indexinfo_p = NULL;
    // TODO: index type 
    fname = arrname + ".chunk_index";
    fh = fopen(fname.data(), "rb");
    if (fh != NULL) {
	
	size_t read_size;
	fsize = get_file_size(fh);
	buf = new char[fsize];
	read_size = fread(buf, 1, fsize, fh);
	assert(read_size == fsize);
	fclose(fh);

	index_meta.assign(buf, fsize);
	deserialize(indexinfo, index_meta);

	index_infos.push_back(indexinfo);
	indexinfo_p = &(index_infos[index_infos.size() - 1]);

	delete buf;
	buf = NULL;
    }

    return indexinfo_p;
}

template <class T>
ArrayDBError ArrayDBHandler::filter_chunks(string& _return, ArrayInfo *arrinfo_p, vector<chunk_type_info_t>& chunk_typeinfos, double min, double max, int type) {

    int chunk_id;
    char *buf;
    size_t bufsize;
    int64_t count;
    string fname;
    struct timeval start, end;
    long sec, usec;
    double io_used_time;
    ChunkType chunk_type;
    size_t nvalid_chunks, ncandidate_chunks;
    T fill_value;

    count = 0;
    io_used_time = 0;
    sec = 0; 
    usec = 0;
    nvalid_chunks = 0;
    ncandidate_chunks = 0;
    fill_value = lexical_cast<T>(arrinfo_p->fill_value);

    bufsize = arrinfo_p->chunks[0].chunk_size;
    buf = (char *)malloc(bufsize);
    assert(buf != NULL);
    
    if (index_type == IndexType::BITMAP_INDEX || index_type == IndexType::CHUNK_BITMAP) {
	printf("bitmap index used\n");
	string path = "./" + arrinfo_p->name;
	CCPart part(path.c_str(), static_cast<const char*>(0));
	uint32_t nelmts;
	ibis::query query(ibis::util::userName(), &part);
	string condition;
	string whereClause;
	string selectClause;

	if (type == LESS)
	    condition = " <= " + lexical_cast<string>(max);
	else if (type == LARGER) 
	    condition = " >= " + lexical_cast<string>(min);
	else
	    condition = " <= " + lexical_cast<string>(max); //TODO:

	for(size_t j = 0; j < chunk_typeinfos.size(); j++) {
	    chunk_id = chunk_typeinfos[j].id;
	    chunk_type = chunk_typeinfos[j].type;

	    if (chunk_type == CANDIDATE_CHUNK) {
		ncandidate_chunks++;
		nelmts = arrinfo_p->chunks[chunk_id].chunk_size / arrinfo_p->dtype_size;
		whereClause = arrinfo_p->name + "_" + lexical_cast<string>(chunk_id) + condition;
		query.setWhereClause(whereClause.c_str());
		selectClause = arrinfo_p->name + "_" + lexical_cast<string>(chunk_id);
		query.setSelectClause(selectClause.c_str());
		part.set_numRows(nelmts);

		ibis::array_t<float> *arr;
		//TODO: other type
		arr = query.getQualifiedFloats(selectClause.c_str());
		if (arr)
		    count += arr->size();
	    }
	    else if (chunk_type == VALID_CHUNK) {
		nvalid_chunks++;
		count += arrinfo_p->chunks[chunk_id].chunk_size / arrinfo_p->dtype_size;
	    }
	    else
		assert(0);
	}
	part.emptyCache();	
    }

    else {
	T *data = (T *)buf;
	filterdata_arg_t *targ;
	if (use_thread_pool)
	    pool_init(nthreads);
	if (fill_value == 0) {
	    for(size_t j = 0; j < chunk_typeinfos.size(); j++) {
		chunk_id = chunk_typeinfos[j].id;
		chunk_type = chunk_typeinfos[j].type;

		/*
	         * thread pool
		 */
		if (use_thread_pool) {
		    string path;
		    char *buf;
		    size_t bufsize;

		    // start time point
		    gettimeofday(&start, NULL);

		    bufsize = (arrinfo_p->compress_type == CompressType::NO_COMPRESSION) ? arrinfo_p->chunks[chunk_id].chunk_size : arrinfo_p->chunks[chunk_id].compressed_size;
		    buf = (char *)malloc(bufsize);
		    assert(buf);
		    path = arrinfo_p->name + "/" + arrinfo_p->name + "_" + lexical_cast<string>(chunk_id);
		    get_disk_chunk_data(buf, bufsize, path.c_str());

		    // end time point
		    gettimeofday(&end, NULL);
		    sec += end.tv_sec - start.tv_sec;
		    usec += end.tv_usec - start.tv_usec;

		    // add job to thread pool
		    targ = (filterdata_arg_t *)malloc(sizeof(filterdata_arg_t));
		    assert(targ);
		    targ->compress_type = arrinfo_p->compress_type;
		    targ->buf = buf;
		    targ->chunkinfo = &(arrinfo_p->chunks[chunk_id]);
		    targ->chunk_type = chunk_type;
		    targ->dtype_size = arrinfo_p->dtype_size;
		    targ->type = type;
		    targ->max = max;
		    targ->min = min;
		    targ->count = &count;
		    targ->ncandidate_chunks = &ncandidate_chunks;
		    targ->nvalid_chunks = &nvalid_chunks;
		    targ->fill_value = &fill_value;
		    pool_add_job(filter_data<T>, targ);
		}

		/*
		 * no thread pool
		 */
		else {
		    // start time point
		    gettimeofday(&start, NULL);

		    get_chunk_data(buf, arrinfo_p->chunks[chunk_id], arrinfo_p->compress_type);

		    // end time point
		    gettimeofday(&end, NULL);
		    sec += end.tv_sec - start.tv_sec;
		    usec += end.tv_usec - start.tv_usec;

		    if (chunk_type == CANDIDATE_CHUNK) {
			// filter data
			int64_t nelmts;

			nelmts = arrinfo_p->chunks[chunk_id].chunk_size / arrinfo_p->dtype_size;

			if (type == LESS) {
			    for (int i = 0; i < nelmts; i++) 
				if (data[i] <= max)
				    count++;
			}
			else if (type == LARGER) {
			    for (int i = 0; i < nelmts; i++) 
				if (data[i] >= min)
				    count++;
			}
			else {
			    for (int i = 0; i < nelmts; i++) 
				if (data[i] >= min && data[i] <= max)
				    count++;
			}

			ncandidate_chunks++;
		    }
		    else if (chunk_type == VALID_CHUNK) {
			count += arrinfo_p->chunks[chunk_id].chunk_size / arrinfo_p->dtype_size;
			nvalid_chunks++;
		    }
		    else
			assert(0);
		}
	    }// end for
	}
	else {
	    for(size_t j = 0; j < chunk_typeinfos.size(); j++) {
		chunk_id = chunk_typeinfos[j].id;
		chunk_type = chunk_typeinfos[j].type;
		if (use_thread_pool) {
		    /*
		     * thread pool
		     */
		    string path;
		    char *buf;
		    size_t bufsize;

		    // start time point
		    gettimeofday(&start, NULL);

		    bufsize = (arrinfo_p->compress_type == CompressType::NO_COMPRESSION) ? arrinfo_p->chunks[chunk_id].chunk_size : arrinfo_p->chunks[chunk_id].compressed_size;
		    buf = (char *)malloc(bufsize);
		    assert(buf);
		    path = arrinfo_p->name + "/" + arrinfo_p->name + "_" + lexical_cast<string>(chunk_id);
		    get_disk_chunk_data(buf, bufsize, path.c_str());

		    // end time point
		    gettimeofday(&end, NULL);
		    sec += end.tv_sec - start.tv_sec;
		    usec += end.tv_usec - start.tv_usec;

		    // add job to thread pool
		    targ = (filterdata_arg_t *)malloc(sizeof(filterdata_arg_t));
		    assert(targ);
		    targ->compress_type = arrinfo_p->compress_type;
		    targ->buf = buf;
		    targ->chunkinfo = &(arrinfo_p->chunks[chunk_id]);
		    targ->chunk_type = chunk_type;
		    targ->dtype_size = arrinfo_p->dtype_size;
		    targ->type = type;
		    targ->max = max;
		    targ->min = min;
		    targ->count = &count;
		    targ->ncandidate_chunks = &ncandidate_chunks;
		    targ->nvalid_chunks = &nvalid_chunks;
		    targ->fill_value = &fill_value;
		    pool_add_job(filter_data<T>, targ);
		}
		else {
		    // start time point
		    gettimeofday(&start, NULL);

		    get_chunk_data(buf, arrinfo_p->chunks[chunk_id], arrinfo_p->compress_type);

		    // end time point
		    gettimeofday(&end, NULL);
		    sec += end.tv_sec - start.tv_sec;
		    usec += end.tv_usec - start.tv_usec;

		    if (chunk_type == CANDIDATE_CHUNK) {
			// filter data
			int64_t nelmts;

			nelmts = arrinfo_p->chunks[chunk_id].chunk_size / arrinfo_p->dtype_size;

			if (type == LESS) {
			    for (int i = 0; i < nelmts; i++) 
				if (data[i] != fill_value && data[i] <= max)
				    count++;
			}
			else if (type == LARGER) {
			    for (int i = 0; i < nelmts; i++) 
				if (data[i] != fill_value && data[i] >= min)
				    count++;
			}
			else {
			    for (int i = 0; i < nelmts; i++) 
				if (data[i] != fill_value && data[i] >= min && data[i] <= max)
				    count++;
			}

			ncandidate_chunks++;
		    }
		    else if (chunk_type == VALID_CHUNK) {
			count += arrinfo_p->chunks[chunk_id].chunk_size / arrinfo_p->dtype_size;
			nvalid_chunks++;
		    }
		    else
			assert(0);
		}
	    }
	}
	if (use_thread_pool)
	    pool_destroy();
    }

    io_used_time = sec + (double)usec / 1000000;
    _return = "count: " + lexical_cast<string>(count) +
	"\nratio: " + lexical_cast<string>((float)count / arrinfo_p->nelmts) +
	"\n candidate chunks: " + lexical_cast<string>(ncandidate_chunks) +
	"\n valid chunks: " + lexical_cast<string>(nvalid_chunks) +
	"\n IO time used: " + lexical_cast<string>(io_used_time) + "\n";
    
    free(buf);
    return SUCCESS;
}

IndexType::type ArrayDBHandler::get_index_type(const string& type) {

    IndexType::type index_type;

    if (type == "chunk_index")
	index_type = IndexType::CHUNK_INDEX;
    else if (type == "bitmap_index")
	index_type = IndexType::BITMAP_INDEX;
    else if (type == "tile_index")
	index_type = IndexType::TILE_INDEX;
    else if (type == "chunk_bitmap")
	index_type = IndexType::CHUNK_BITMAP;
    else if (type == "chunk_tile")
	index_type = IndexType::CHUNK_TILE;
    else
        index_type = IndexType::NO_INDEX;

    return index_type;
}


void ArrayDBHandler::set_fillvalue(std::string& _return, const std::string& array_name, const std::string& fill_value) {
    ArrayInfo *arrinfo_p;

    arrinfo_p = get_arrinfo(array_name);
    assert(arrinfo_p);

    arrinfo_p->fill_value = fill_value;
    write_arrinfo(*arrinfo_p);
    _return = array_name + " fill value: " + fill_value + "\n";
}


ArrayDBError ArrayDBHandler::get_disk_chunk_data(void *buf, size_t size, const char *filename) {
    FILE *fh;
    size_t bytes_read;

    fh = fopen(filename, "rb");
    if (fh == NULL) {
	return CHUNK_DATA_NOT_FIND;
    }

    bytes_read = fread(buf, 1, size, fh);
    assert(bytes_read == size);
    fclose(fh);
    fh = NULL;

    return SUCCESS;
}

ArrayDBError ArrayDBHandler::put_chunk_data(const void *buf, ChunkInfo& chunkinfo, CompressType::type compress_type) {
    ArrayDBError err;
    FILE *fh;
    string path;

    err = SUCCESS;
    path = chunkinfo.chunk_name + "/" + chunkinfo.chunk_name + "_" + lexical_cast<string>(chunkinfo.chunk_id);
    fh = fopen(path.c_str(), "wb");
    assert(fh != NULL);
    
    if (compress_type == CompressType::NO_COMPRESSION) {
	fwrite(buf, 1, chunkinfo.chunk_size, fh);
    }
    else {
	char *dest_buf;
	size_t cmps_size;

	dest_buf = (char *)malloc(chunkinfo.chunk_size);
	assert(dest_buf);
	
	// decompress data
	Compressor *compressor;
	compressor = (CompressorFactory::getInstance().getCompressors())[compress_type];
	
	cmps_size = compressor->compress(dest_buf, chunkinfo.chunk_size, buf);
	assert(cmps_size);
	
	chunkinfo.compressed_size = cmps_size;
	fwrite(dest_buf, 1, cmps_size, fh);
	free(dest_buf);
    }
    
    fclose(fh);
    return err;
}

ArrayDBError ArrayDBHandler::get_chunk_data(void *buf, ChunkInfo& chunkinfo, CompressType::type compress_type) {
    ArrayDBError err;
    string path;

    path = chunkinfo.chunk_name + "/" + chunkinfo.chunk_name + "_" + lexical_cast<string>(chunkinfo.chunk_id);
    if (compress_type == CompressType::NO_COMPRESSION)
	err =  get_disk_chunk_data(buf, chunkinfo.chunk_size, path.c_str());
    else {
	char *cmps_buf;
	cmps_buf = (char *)malloc(chunkinfo.compressed_size);
	assert(cmps_buf);
	
	// get compressed chunk data
	err =  get_disk_chunk_data(cmps_buf, chunkinfo.compressed_size, path.c_str());
	
	// decompress data
	Compressor *compressor;
	compressor = (CompressorFactory::getInstance().getCompressors())[compress_type];
	if (!(compressor->decompress(buf, chunkinfo.chunk_size, cmps_buf, chunkinfo.compressed_size)))
	    err = DECOMPRESS_FAILED;
	free(cmps_buf);
    }

    assert(err == SUCCESS);
    return err;
}

void ArrayDBHandler::set_para(std::string& _return, const std::string& key, const std::string& value) {
    if (key == "time")
	is_count_time = (value == "off") ? false : true;
    else if (key == "indextype")
	index_type = get_index_type(value);
    else if (key == "nthreads")
	nthreads = lexical_cast<int>(value);
    else if (key == "threadpool")
	use_thread_pool = (value == "off") ? false : true;
    else {
	_return = key + " is not supported\n";
	return;
    }
    _return = "set " + key + " " + value + "\n";
}

template <class T>
void *filter_data(void *arg) {
    filterdata_arg_t *targ = (filterdata_arg_t *)arg;
    char *buf;
    ArrayDBError err;

    if (targ->compress_type == CompressType::NO_COMPRESSION)
	buf = targ->buf;
    else {
	buf = (char *)malloc(targ->chunkinfo->chunk_size);
	assert(buf);

	// decompress data
	Compressor *compressor;
	compressor = (CompressorFactory::getInstance().getCompressors())[targ->compress_type];
	if (!(compressor->decompress(buf, targ->chunkinfo->chunk_size, targ->buf, targ->chunkinfo->compressed_size)))
	    err = DECOMPRESS_FAILED;
	free(targ->buf);
    }

    T *data = (T *)buf;
    int64_t count = 0;
    T fill_value = *((T *)(targ->fill_value));
    if (targ->chunk_type == CANDIDATE_CHUNK) {
	// filter data
	int64_t nelmts;

	nelmts = targ->chunkinfo->chunk_size / targ->dtype_size;
	if (fill_value == 0) {
	    if (targ->type == LESS) {
		for (int i = 0; i < nelmts; i++) 
		    if (data[i] <= targ->max)
			count++;
	    }
	    else if (targ->type == LARGER) {
		for (int i = 0; i < nelmts; i++) 
		    if (data[i] >= targ->min)
			count++;
	    }
	    else {
		for (int i = 0; i < nelmts; i++) 
		    if (data[i] >= targ->min && data[i] <= targ->max)
			count++;
	    }
	}
	else {
	    if (targ->type == LESS) {
		for (int i = 0; i < nelmts; i++) 
		    if (data[i] != fill_value && data[i] <= targ->max)
			count++;
	    }
	    else if (targ->type == LARGER) {
		for (int i = 0; i < nelmts; i++) 
		    if (data[i] != fill_value && data[i] >= targ->min)
			count++;
	    }
	    else {
		for (int i = 0; i < nelmts; i++) 
		    if (data[i] != fill_value && data[i] >= targ->min && data[i] <= targ->max)
			count++;
	    }
	}
	pool_arg_lock();
	*(targ->count) += count;
	(*(targ->ncandidate_chunks))++;
	pool_arg_unlock();
    }
    else if (targ->chunk_type == VALID_CHUNK) {
	count += targ->chunkinfo->chunk_size / targ->dtype_size;

	pool_arg_lock();
	*(targ->count) += count;
	(*(targ->nvalid_chunks))++;
	pool_arg_unlock();
    }
    else
	assert(0);

    free(buf);
    free(targ);
    
    return NULL;
}

void *gen_chunk_index(void *arg) {
    genindex_arg_t *targ = (genindex_arg_t *)arg;
    char *buf;
    ArrayDBError err;

    if (targ->compress_type == CompressType::NO_COMPRESSION)
	buf = targ->buf;
    else {
	buf = (char *)malloc(targ->chunkinfo->chunk_size);
	assert(buf);

	// decompress data
	Compressor *compressor;
	compressor = (CompressorFactory::getInstance().getCompressors())[targ->compress_type];
	if (!(compressor->decompress(buf, targ->chunkinfo->chunk_size, targ->buf, targ->chunkinfo->compressed_size)))
	    err = DECOMPRESS_FAILED;
	free(targ->buf);
    }


    double max, min;
    int64_t nelmts = targ->chunkinfo->chunk_size / targ->dtype_size;
    get_chunk_data_range(min, max, buf, nelmts, targ->dtype, *(targ->fill_value));
    
    targ->chunk_index->max = max;
    targ->chunk_index->min = min;

   
    free(buf);
    free(targ);
    return NULL;
}
