/***********************************************************************
 * Filename : arraydb_support.h
 * Create : XXX 2011-11-16
 * Created Time: 2011-11-16
 * Description: 
 * Modified   : 
 * **********************************************************************/

#include "arraydb_support.h"

/*template <class T>
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
}*/

size_t get_file_size(FILE *fh) {
    assert(fh != NULL);

    size_t size;
    fseek(fh, 0, SEEK_END);
    size = ftell(fh);
    fseek(fh, 0, SEEK_SET);

    return size;
}

template <class T>
void data_to_string(string& data, T *buf, int64_t nelmts) {
    int64_t i, j, ncols;

    ncols = 10;
    for (i = 0; i < nelmts;) {
	data += "(" + lexical_cast<string>(i / 10 + 1) + "): ";
	for (j = 0; j < ncols && i < nelmts; j++)
	    data += lexical_cast<string>(buf[i++]) + " ";
	data += "\n";
    }
}

void bin_to_string(string& data, char *buf, DataType::type dtype, int64_t nelmts) {
    data = "";
    switch (dtype) {
	case DataType::FLOAT:
	    data_to_string<float>(data, (float *)buf, nelmts);
	    break;
	case DataType::DOUBLE:
	    data_to_string<double>(data, (double *)buf, nelmts);
	    break;
	case DataType::INT32:
	    data_to_string<int>(data, (int *)buf, nelmts);
	    break;
	case DataType::INT64:
	    data_to_string<int64_t>(data, (int64_t *)buf, nelmts);
	    break;
	case DataType::INT16:
	    data_to_string<int16_t>(data, (int16_t *)buf, nelmts);
	    break;
	default:
	    data = lexical_cast<string>(dtype) + " not implementation yet!";
	    break;
    }

}

/* TODO: ugly interface */
DataType::type get_datatype(const string& str, int64_t *dtype_size) {
    DataType::type dtype = DataType::UNKNOWN;

    if (str == "int16") {
	*dtype_size = 2;
	dtype =  DataType::INT16;
    }
    else if (str == "uint16") {
	*dtype_size = 2;
	dtype =  DataType::UINT16;
    }
    if (str == "int32") {
	*dtype_size = 4;
	dtype =  DataType::INT32;
    }
    else if (str == "uint32") {
	*dtype_size = 4;
	dtype =  DataType::UINT32;
    }
    if (str == "int64") {
	*dtype_size = 8;
	dtype =  DataType::INT64;
    }
    else if (str == "uint64") {
	*dtype_size = 8;
	dtype =  DataType::UINT64;
    }
    if (str == "float") {
	*dtype_size = 4;
	dtype =  DataType::FLOAT;
    }
    else if (str == "double") {
	*dtype_size = 8;
	dtype =  DataType::DOUBLE;
    }

    return dtype;
}

string arraydb_error(ArrayDBError status) {
    string ret;

    switch (status) {
	case ARRAY_NOT_EXIST:
	    ret = "array meta not finded";
	    break;
	case SUBARRAY_PARAMS_ERROR:
	    ret = "subarray parameters num error";
	    break;
	case COORDS_OVERFLOW:
	    ret = "subarray coords overflow the array coords";
	    break;
	case CHUNK_DATA_NOT_FIND:
	    ret = "chunk data not finded";
	    break;
	default:
	    ret = "unknown error";
	    break;
    }

    return ret;
}

string get_type_str(DataType::type type) {
    string ret;
    switch (type) {
	case DataType::INT16:
	    ret = "int16";
	    break;
	case DataType::UINT16:
	    ret = "uint16";
	    break;
	case DataType::INT32:
	    ret = "int32";
	    break;
	case DataType::UINT32:
	    ret = "uint32";
	    break;
	case DataType::INT64:
	    ret = "int64";
	    break;
	case DataType::UINT64:
	    ret = "uint64";
	    break;
	case DataType::FLOAT:
	    ret = "float";
	    break;
	case DataType::DOUBLE:
	    ret = "double";
	    break;
	default:
	    ret = "unknown type";
	    break;
    }
    return ret;
}


template <class T>
void get_chunk_data_range_by_type(double& min, double& max, T *data, int64_t nelmts, string& fv_str) {
    
    T max_t, min_t, fill_value;
    
    max_t = -numeric_limits<T>::max();
    min_t = numeric_limits<T>::max();
    fill_value = lexical_cast<T>(fv_str);

    if (fill_value == 0) {
	for (int64_t i = 0; i < nelmts; i++) {
	    if (data[i] > max_t)
		max_t = data[i];
	    if (data[i] < min_t)
		min_t = data[i];
	}
    }
    else {
	for (int64_t i = 0; i < nelmts; i++) {
	    if (data[i] != fill_value) {
		if (data[i] > max_t)
		    max_t = data[i];
		if (data[i] < min_t)
		    min_t = data[i];
	    }
	}
    }
    min = min_t;
    max = max_t;
}

void get_chunk_data_range(double& min, double& max, char *data, int64_t nelmts, DataType::type dtype, string& fill_value) {
    switch (dtype) {
	case DataType::FLOAT:
	    get_chunk_data_range_by_type<float>(min, max, (float *)data, nelmts, fill_value);
	    break;
	case DataType::DOUBLE:
	    get_chunk_data_range_by_type<double>(min, max, (double *)data, nelmts, fill_value);
	    break;
	case DataType::INT32:
	    get_chunk_data_range_by_type<int>(min, max, (int *)data, nelmts, fill_value);
	    break;
	case DataType::INT64:
	    get_chunk_data_range_by_type<int64_t>(min, max, (int64_t *)data, nelmts, fill_value);
	    break;
	case DataType::INT16:
	    get_chunk_data_range_by_type<int16_t>(min, max, (int16_t *)data, nelmts, fill_value);
	    break;
	default:
	    printf("data type: %d\n", dtype);
	    assert(0);
	    break;
    }
   
}
