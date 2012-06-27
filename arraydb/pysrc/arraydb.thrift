namespace cpp arraydb

enum DataType {
    UNKNOWN = 0,
    INT16,
    UINT16,
    INT32,
    UINT32,
    INT64,
    UINT64,
    FLOAT,
    DOUBLE
}

enum CompressType {
    NO_COMPRESSION = 0,
    ZLIB,
    LZMA
}

enum StorageStrategy {
    ROUND_ROBIN = 1
}

enum IndexType {
    CHUNK_INDEX = 0,
    BITMAP_INDEX,
    TILE_INDEX,
    CHUNK_BITMAP,
    CHUNK_TILE,
    NO_INDEX
}

struct Array {
    1: string name,
    2: CompressType compress_type, 
    3: binary data
}

struct ChunkInfo {
    1: i32 chunk_id,
    2: i32 node_id,
    3: i64 ndims,
    4: i64 chunk_size,
    5: i64 compressed_size,
    6: string chunk_name,
    7: list<i64> first,
    8: list<i64> last
}

struct DimInfo {
    1: string name,
    2: i64 length,
    3: i64 chunk_length,
    4: i64 overlap_length
}

struct ArrayInfo {
    1: i64 array_id,
    2: string name,
    3: string var_name,
    4: DataType var_type,
    5: i64 dtype_size,
    6: i64 nelmts,
    7: i64 ndims,
    8: StorageStrategy ss,
    9: i64 nchunks,
    10: list<DimInfo> dims,
    11: list<i64> chunk_dims,
    12: list<ChunkInfo> chunks,
    13: string fill_value
    14: CompressType compress_type, 
}

struct ChunkIndex {
    1: i32 chunk_id;
    2: double max;
    3: double min;
}

struct IndexInfo {
    1: string array_name;
    2: string index_name;
    3: IndexType index_type;
    4: list<ChunkIndex> chunk_indexs;
}


service ArrayDB {
    string executeQuery(1: string request, 2: string data),
    string store(1: Array array, 2: string array_name),
    string load_chunk(1: string name, 2: i32 id, 3: list<i64> first, 4: list<i64> last),
    void create_arrinfo(1: ArrayInfo arrinfo),
    Array load(1: string array_name)
    string create_indexinfo(1: string array_name, 2:string index_name, 3: IndexType index_type)
    string filter(1: string array_name, 2:string var_name, 3: string min, 4: string max)
    string set_fillvalue(1: string chunk_name, 2: string fill_value)
    string set_para(1: string key, 2: string value)
    oneway void test_nonblocking()
}

    //string set_time(1: bool flag)
    //string set_index_type(1: IndexType index_type)
    //string echo(1: string request),	
