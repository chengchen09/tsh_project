/***********************************************************************
* Filename : locality_indice.cpp
* Create : XXX 2012-02-18
* Created Time: 2012-02-18
* Description: 
* Modified   : 
* **********************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
#include <assert.h>
#include <string>	/* string in c++ not in c(string.h) */
#include <iostream>
#include <vector>
#include <limits>

#define UNKNOWN 0
#define INT16	1
#define UINT16	2
#define INT32	3
#define UINT32	4
#define INT64	5
#define UINT64	6
#define FLOAT	7
#define DOUBLE	8

using namespace std;

void usage() {
    printf("Usage: locality_indice infile datatype ndims array_dims[...] chunk_dims[...] fill_value\n");
}

int count_chunk_id_by_elmt_index(int ndims, int *array_dims, int *chunk_dims, int *chunk_nums, size_t elmt_index) {
    std::vector<int> chunk_coords(ndims, 0);
    int i, j;
    int pos, chunk_id, coord;

    /* count chunk_coords */
    pos = elmt_index;
    chunk_id = 0;
    for (i = 0, j = ndims - 1; i < ndims; i++, j--) {
	coord = pos % array_dims[j];
	pos = pos / array_dims[j];
	chunk_coords[j] = coord / chunk_dims[j];
    }
    
    chunk_id = chunk_coords[0];
    for (i = 1; i < ndims; i++)
	chunk_id = chunk_id * chunk_nums[i] + chunk_coords[i];

    return chunk_id;
}



template <class T>
void count_locality_indice(T *data, size_t nitems, int ndims,  int *array_dims, int *chunk_dims, T fill_value) {
    vector<T> range;
    int *chunk_nums;
    int nchunks;
    double average_range, array_range, locality_indice;
    T array_max, array_min;
    T *chunk_max, *chunk_min;

    chunk_nums = new int[ndims];

    nchunks = 1;
    for (int i = 0; i < ndims; i++) {
	chunk_nums[i] = array_dims[i] / chunk_dims[i] + (array_dims[i] % chunk_dims[i] ? 1 : 0);
	nchunks *= chunk_nums[i];
    }
    
    chunk_max = new T[nchunks];
    chunk_min = new T[nchunks];
    for (int i = 0; i < nchunks; i++) {
	chunk_max[i] = -numeric_limits<T>::max();
	chunk_min[i] = numeric_limits<T>::max();
    }

    average_range = 0;
    array_max = -numeric_limits<T>::max();
    array_min = numeric_limits<T>::max();

    int chunk_pos;
    if(fill_value == 0) {
	for (size_t i = 0; i < nitems; i++) {
	    // find the chunk the element come from
	    chunk_pos = count_chunk_id_by_elmt_index(ndims, array_dims, chunk_dims, chunk_nums, i);
	    if (chunk_min[chunk_pos] > data[i])
		chunk_min[chunk_pos] = data[i];
	    if (chunk_max[chunk_pos] < data[i])
		chunk_max[chunk_pos] = data[i];

	    if (array_min > data[i])
		array_min = data[i];
	    if (array_max < data[i])
		array_max = data[i];
	}
    }
    else {
	for (size_t i = 0; i < nitems; i++) {
	    if (data[i] != fill_value) {
		// find the chunk the element come from
		chunk_pos = count_chunk_id_by_elmt_index(ndims, array_dims, chunk_dims, chunk_nums, i);
		if (chunk_min[chunk_pos] > data[i])
		    chunk_min[chunk_pos] = data[i];
		if (chunk_max[chunk_pos] < data[i])
		    chunk_max[chunk_pos] = data[i];

		if (array_min > data[i])
		    array_min = data[i];
		if (array_max < data[i])
		    array_max = data[i];
	    }
	}
    }

    
    for (int i = 0; i < nchunks; i++) {
	if (chunk_max[i] != -numeric_limits<T>::max()) {
	    average_range += chunk_max[i] - chunk_min[i];
	    range.push_back(chunk_max[i] - chunk_min[i]);
	    //printf("%d: %d %d %d\n", i, chunk_max[i], chunk_min[i], chunk_max[i] - chunk_min[i]);
	    cout<<i<<" "<<chunk_min[i]<<" "<<chunk_max[i]<<endl;
	}
    }

    average_range /= range.size();
    array_range = array_max - array_min;
    locality_indice = average_range / array_range;

    printf("array dims: %d ", array_dims[0]);
    for (int i = 1; i < ndims; i++)
	printf("* %d ", array_dims[i]);
    printf("\nchunk dims: %d ", chunk_dims[0]);
    for (int i = 1; i < ndims; i++)
	printf("* %d ", chunk_dims[i]);

    printf("\nall chunk num: %d\n", nchunks);
    printf("available chunk num: %lu\n", range.size());
    printf("chunk average range: %f\n", average_range);
    printf("array range: %f\n", array_range);
    printf("locality_indice: %f\n", locality_indice);

    delete []chunk_max;
    delete []chunk_min;
    delete []chunk_nums;
}

int main(int argc, char **argv)
{
    if (argc < 4) {
	usage();
	exit(1);
    }

    int dtype;
    FILE *fp;
    char *buf;
    long fsize;
    int ndims;
    int *chunk_dims, *array_dims;
    size_t tsize, nitems;
    string typestr(argv[2]);
   
    ndims = atoi(argv[3]);
    chunk_dims = new int[ndims];
    array_dims = new int[ndims];

    if (typestr == "float") {
	dtype = FLOAT;
	tsize = 4;
    }
    else if (typestr == "double") {
	dtype = DOUBLE;
	tsize = 8;
    }
    else if (typestr == "int32") {
	dtype = INT32;
	tsize = 4;
    }
    else {
	dtype = UNKNOWN;
	tsize = 0;
    }
    if (dtype == UNKNOWN) {
	cerr<<"data type is not reconized\n";
	usage();
	exit(1);
    }

    nitems = 1;
    for (int i = 0; i < ndims; i++) {
	array_dims[i] = atoi(argv[4 + i]);
	chunk_dims[i] = atoi(argv[4 + ndims + i]);
	nitems *= array_dims[i];
    }

    /* read file */
    fp = fopen(argv[1], "rb");
    assert(fp != NULL);
    
    /*fseek(fp, 0, SEEK_END);
    fsize = ftell(fp);
    assert(fsize >= 0);
    fseek(fp, 0, SEEK_SET);*/
    
    fsize = nitems * tsize;
    buf =  (char *)malloc(fsize);
    assert(buf != NULL);
    fread(buf, tsize, nitems, fp);
    fclose(fp);

    if (typestr == "float") {
	float fill_value = 0;
	if (argc > 4 + 2 * ndims) 
	    fill_value = atof(argv[4 + 2 * ndims]);
	else
	    fill_value = 0;
	count_locality_indice<float>((float *)buf, nitems, ndims, array_dims, chunk_dims, fill_value);
    }
    else if (typestr == "double") {
	double fill_value = 0;
	if (argc > 4 + 2 * ndims) 
	    fill_value = atof(argv[4 + 2 * ndims]);
	else
	    fill_value = 0;
	count_locality_indice<double>((double *)buf, nitems, ndims, array_dims, chunk_dims, fill_value);
    }
    else if (typestr == "int32") {
	int fill_value = 0;
	if (argc > 4 + 2 * ndims) 
	    fill_value = atoi(argv[4 + 2 * ndims]);
	else
	    fill_value = 0;
	count_locality_indice<int>((int *)buf, nitems, ndims, array_dims, chunk_dims, fill_value);
    }

    free(buf);
    delete [] chunk_dims;
    delete [] array_dims;
    return 0;
}
