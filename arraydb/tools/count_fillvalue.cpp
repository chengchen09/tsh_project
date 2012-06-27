/***********************************************************************
* Filename : count_fillvalue.cpp
* Create : XXX 2012-03-01
* Created Time: 2012-03-01
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
    printf("Usage: count_fillvalue infile datatype fill_value\n");
}

template <class T>
void count_fillvalue(T *data, size_t nitems, T fill_value) {
    size_t i;
    size_t sum = 0;

    for (i = 0; i < nitems; i++)
	if (data[i] == fill_value)
	    sum ++;
    printf("All elments: %lu\n", nitems);
    printf("file value: %lu\n", sum);
    printf("fill_value percentage: %f\n", double(sum) / nitems);
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
    size_t tsize, nitems;
    string typestr(argv[2]);
   
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

    /* read file */
    fp = fopen(argv[1], "rb");
    assert(fp != NULL);
    
    fseek(fp, 0, SEEK_END);
    fsize = ftell(fp);
    assert(fsize >= 0);
    fseek(fp, 0, SEEK_SET);
    
    nitems = fsize / tsize;
    buf =  (char *)malloc(fsize);
    assert(buf != NULL);
    fread(buf, tsize, nitems, fp);
    fclose(fp);

    if (typestr == "float") {
	float fill_value = 0;
	fill_value = atof(argv[3]);
	count_fillvalue<float>((float *)buf, nitems, fill_value);
    }
    else if (typestr == "double") {
	double fill_value = 0;
	fill_value = atof(argv[3]);
	count_fillvalue<double>((double *)buf, nitems, fill_value);
    }
    else if (typestr == "int32") {
	int fill_value = 0;
	fill_value = atoi(argv[3]);
	count_fillvalue<int>((int *)buf, nitems, fill_value);
    }

    free(buf);
    return 0;
}
