/***********************************************************************
* Filename : bincat.c
* Create : XXX 2011-08-25
* Created Time: 2011-08-25
* Description: 
* Modified   : 
* **********************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string>	/* string in c++ not in c(string.h) */
#include <iostream>

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

void print_bin_data(void *buf, int dtype, size_t nitems);
void print_float_data(float *data, size_t nitems);
void print_int32_data(int *data, size_t nitems);
void print_pos_data(void *buf, int dtype, size_t pos);

void usage()
{
    printf("Usage: bincat datatype infile\n");
}

int main(int argc, char **argv)
{
    if (argc < 3) {
	usage();
	exit(1);
    }

    int dtype;
    FILE *fp;
    char *buf;
    long fsize;
    size_t tsize, nitems, pos;
    string typestr(argv[1]);
   
    if (argc == 4)
	pos = atoi(argv[3]);
    else 
	pos = 0;

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
    fp = fopen(argv[2], "rb");
    assert(fp != NULL);
    
    fseek(fp, 0, SEEK_END);
    fsize = ftell(fp);
    assert(fsize >= 0);
    fseek(fp, 0, SEEK_SET);
    
    nitems = fsize / tsize;
    buf = new char[fsize];
    assert(buf != NULL);
    fread(buf, tsize, nitems, fp);
    fclose(fp);

    /* TODO: print data */
    if (pos > 0) {
	assert(pos > 0 && pos < nitems);
	print_pos_data(buf, dtype, pos - 1);
    }
    else
	print_bin_data(buf, dtype, nitems);

    return 0;
}

void print_pos_data(void *buf, int dtype, size_t pos)
{
    switch(dtype) {
	case FLOAT:
	    cout<<((float *)buf)[pos]<<endl;
	    break;
	default:
	    break;
    }
}

void print_bin_data(void *buf, int dtype, size_t nitems)
{
    switch(dtype) {
	case FLOAT:
	    print_float_data((float *)buf, nitems);
	    break;
	case DOUBLE:
	    //print_double_data((double *)buf);
	    break;
	case INT32:
	    print_int32_data((int *)buf, nitems);
	    break;
	default:
	    break;
    }

}

void print_float_data(float *data, size_t nitems)
{
    size_t i;
    int j, ncols;

    ncols = 5;
    for (i = 0; i < nitems;) {
	for (j = 0; j < ncols && i < nitems; j++)
	    cout<<data[i++]<<"\t";
	cout<<"\n";
    }
}

void print_int32_data(int *data, size_t nitems) {
    size_t i;
    int j, ncols;

    ncols = 5;
    for (i = 0; i < nitems;) {
	for (j = 0; j < ncols && i < nitems; j++)
	    cout<<data[i++]<<"\t";
	cout<<"\n";
    }
}
