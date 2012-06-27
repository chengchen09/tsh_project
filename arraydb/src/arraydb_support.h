/***********************************************************************
 * Filename : arraydb_support.h
 * Create : XXX 2011-11-16
 * Created Time: 2011-11-16
 * Description: 
 * Modified   : 
 * **********************************************************************/

#ifndef ARRAYDB_SUPPORT_H
#define ARRAYDB_SUPPORT_H

#include "global.h"

/*template <class T>
void serialize(string &str, const T &obj);

template <class T>
void deserialize(T &obj, const string& buf);*/

size_t get_file_size(FILE *fh);

template <class T>
void data_to_string(string& data, T *buf, int64_t nelmts);

void bin_to_string(string& data, char *buf, DataType::type dtype, int64_t nelmts);

/* TODO: ugly interface */
DataType::type get_datatype(const string& str, int64_t *dtype_size);

string arraydb_error(ArrayDBError status);

string get_type_str(DataType::type type);

// used by get_chunk_data_range
template <class T>
void get_chunk_data_range_by_type(double& min, double& max, T *data, int64_t nelmts); 

// find the min and max value in chunk_data
void get_chunk_data_range(double& min, double& max, char *data, int64_t nelmts, DataType::type dtype, string& fill_value); 


#endif
