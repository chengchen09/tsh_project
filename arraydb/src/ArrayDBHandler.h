/***********************************************************************
 * Filename : ArrayDBHandler.h
 * Create : chen 2011-11-16
 * Created Time: 2011-11-16
 * Description: 
 * Modified   : 
 * **********************************************************************/
#ifndef ARRAYDB_HANDLER_H
#define ARRAYDB_HANDLER_H

#include "global.h"
#include "arraydb_support.h"
#include "cc_part.h"
#include "Compressor.h"

//namespace arraydb {

class ServerConnection {
    private:	
	RoleType role;
	string address;
	string ip;
	int node_id;
	int port;
	shared_ptr<TTransport> transport;
	shared_ptr<ArrayDBClient> client;
    public:
	ServerConnection(RoleType r, string addr, int id): role(r), address(addr), node_id(id) {
	    std::vector<string> strs; 
	    boost::split(strs, addr, is_any_of(":"), token_compress_off);
	    ip = strs[0];
	    port = lexical_cast<int>(strs[1].data());

	    shared_ptr<TTransport> socket(new TSocket(ip, port));
	    shared_ptr<TTransport> ts(new TFramedTransport(socket));
	    transport = ts;
	    shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
	    shared_ptr<ArrayDBClient> clt(new ArrayDBClient(protocol));
	    client = clt;
	}

	~ServerConnection() {
	}

	shared_ptr<ArrayDBClient> get_client() {
	    /* TODO: everytime open */
	    transport->open();
	    return client;
	}
	string get_address() {
	    return address;
	}
	int get_nodeid() {
	    return node_id;
	}
};


class ArrayDBHandler : virtual public ArrayDBIf {
    private:
	int local_id;
	RoleType  local_role;
	bool is_count_time;
	IndexType::type index_type;
	std::vector<ServerConnection> clients;
	std::vector<ArrayInfo> array_infos;
	std::vector<IndexInfo> index_infos;
	int nthreads;
	bool use_thread_pool;

    public:
	ArrayDBHandler(int id, RoleType role, std::vector<ServerConnection>& clts): local_id(id), local_role(role) {
	    clients = clts;
	    is_count_time = true;
	    index_type = IndexType::NO_INDEX;
	    nthreads = 8;
	    use_thread_pool = true;
	}
	RoleType get_local_role() {
	    return local_role;
	}

	void executeQuery(std::string& _return, const std::string& request, const std::string& data);

	void echo(std::string& _return, const std::string& request); 
	void store(std::string& _return, const Array& array, const std::string& array_name);
	void create_arrinfo(const ArrayInfo& arrinfo);
	void load(Array& _return, const std::string& array_name);
	/* TODO: load_chunk */
	void load_chunk(std::string& _return, const std::string& name, const int32_t id, const std::vector<int64_t> & first, const std::vector<int64_t> & last);

	void create_indexinfo(std::string& _return, const std::string& array_name, const std::string& index_name, const IndexType::type index_type);
	void filter(std::string& _return, const std::string& array_name, const std::string& var_name, const std::string& min, const std::string& max);

	void set_time(std::string& _return, const bool flag) {
	    if (flag) {
		is_count_time = true;
		_return = "set count time on\n";
	    }
	    else {
		is_count_time = false;
		_return = "set count time off\n";
	    }
	}

	void set_index_type(std::string& _return, const IndexType::type type) {
	    index_type = type;
	    _return = "index type: " + lexical_cast<string>(type) + "\n";
	}
	
	void set_fillvalue(std::string& _return, const std::string& chunk_name, const std::string& fill_value); 

	

    private:
	/* write array metadata into dist */
	void write_arrinfo(const ArrayInfo& arrinfo);
	
	/* search array metadata through array name */
	ArrayInfo* get_arrinfo(const string& arrname); 
	/* print array metadata, for debug */
	void print_array_info(const ArrayInfo& arrinfo);
	/* parse subarray infomation */
	ArrayDBError parse_subarray(const string& query, string& name, std::vector<int64_t>& first, std::vector<int64_t>& last); 

	/* master distribute array to workers */ 
	ArrayDBError distribute_array(string& _rieturn, const string& name, const string& data, CompressType::type compress_type);

	/* check if subarray dimension is over array dimension */  
	ArrayDBError check_overflow(const string& name, std::vector<int64_t>& first, std::vector<int64_t>& last); 

	/* schedule subarray */ 
	ArrayDBError execute_subarray(string& data, const string& name, vector<int64_t>& first, vector<int64_t>& last); 
	
	/* get chunkids whose dimension is in the subarray dimension */
	void get_subarray_chunkids(std::vector<int>& chunk_ids, const ArrayInfo& arrinfo, const std::vector<int64_t>& first, const std::vector<int64_t>& last); 
	
	/* gather chunk from workers */
	void gather_chunk_data(string &data, const ArrayInfo& arrinfo, const std::vector<int>& chunk_ids, const std::vector<int64_t>& first, const std::vector<int64_t>& last);
	
	/* get the chunkid of an array cell through its index */
	size_t count_chunk_id_by_elmt_index(const ArrayInfo *arrinfo_p, size_t elmt_index);

	/* get the chunkid through chunk index */
	int count_chunk_id_by_chunk_index(const ArrayInfo& arrinfo, std::vector<int64_t>& first_chunk, std::vector<int64_t>& chunk_lens, int index);

	/* get the chunkid throught chunk coordinates */
	inline int count_chunk_id_by_chunk_coords(const ArrayInfo& arrinfo, std::vector<int64_t>& chunk_coords);
	int get_index_by_chunk_id(const std::vector<int>& chunk_ids, int chunk_id); 
	int get_node_id_by_chunk_id(const ArrayInfo& arrinfo, int chunk_id);
	ServerConnection * get_node_handler(int node_id);
	/* map chunk to worker node */
	void map_chunk(ArrayInfo& arrinfo, StorageStrategy::type ss);
	
	/* use round robin to map chunk */
	void map_chunk_round_robin(ArrayInfo& arrinfo);

	// parse create array
	void parse_create_array(string& query, string& _return);
	
	/*
	 * handle index part
	 */
	// parse create index
	void parse_create_index(string& query, string& _return);

	// create index info from a chunk
	void create_chunk_indexinfo(IndexInfo& indexinfo, ChunkInfo& chunkinfo, DataType::type dtype, string& fill_value);

	// create chunk index
	void create_chunk_index(std::string& _return, const std::string& array_name, const std::string& index_name); 

	// create bitmap index
	void create_bitmap_index(std::string& _return, const std::string& array_name, const std::string& index_name); 

	// write index info to dist 
	void write_indexinfo(const IndexInfo& indexinfo);
	// find index info according array name
	IndexInfo* get_indexinfo(const string& arrname);
	IndexType::type get_index_type(const string& type);

	/*
	 * select clause part
	 */
	void parse_select(string& _return, string& query);
	void set_min_max(string& min, string& max, string& query);
	
	template <class T>
	ArrayDBError filter_chunks(string& ret, ArrayInfo *arrinfo_p, vector<chunk_type_info_t>& chunk_typeinfos, double min, double max, int type);

	/*
	 * set clause part
	 */
	void parse_set_clause(string& _return, string& q);
	void set_para(std::string& _return, const std::string& key, const std::string& value);

	/*
	 * put/get chunk_data
	 */
	ArrayDBError get_disk_chunk_data(void *buf, size_t size, const char *filename); 
	ArrayDBError put_chunk_data(const void *buf, ChunkInfo& chunkinfo, CompressType::type compress_type);
	ArrayDBError get_chunk_data(void *buf, ChunkInfo& chunkinfo, CompressType::type compress_type); 
}; // end of class ArrayDBHandler


//} //end of namspace arraydb


#endif
