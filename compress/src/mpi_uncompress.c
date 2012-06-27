/***********************************************************************
 * Filename : mpi_uncompress.c
 * Create : Chen Cheng 2011-06-09
 * Description: 
 * Modified   : 
 * Revision of last commit: $Rev$ 
 * Author of last commit  : $Author$ $date$
 * licence :
 $licence$
 * **********************************************************************/
#include <mpi.h>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "LzmaLib.h"
#include "Types.h"

#define NAME_SIZE 1024
#define DATA_OFFSET 16

int mpi_size;
int mpi_rank;

char *get_fhname(char *name);
char *get_fwname(char *name);
void error_print(int err_code);

int main(int argc, char **argv) {
    if(argc < 2) {
	printf("Usage: %s infile\n", argv[0]);
	exit(1);
    }

    MPI_Comm comm = MPI_COMM_WORLD;
    MPI_Info mpi_info = MPI_INFO_NULL;
    MPI_File fh, fw;
    MPI_Offset file_size;
    MPI_Status status;
    int retval;
    double start, end;

    unsigned char *buf, *outbuf, *props;
    size_t destlen, read_size;
    size_t propsize = 5;
    unsigned long long *origin_size;
    char *fhname;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(comm, &mpi_rank);
    MPI_Comm_size(comm, &mpi_size);

    /*
     * get file size
     */
    fhname = get_fhname(argv[1]);
    retval = MPI_File_open(MPI_COMM_SELF, fhname, MPI_MODE_RDONLY, mpi_info, &fh);
    assert(retval == MPI_SUCCESS);
    MPI_File_get_size(fh, &file_size);
    //printf("file size:%d\n", file_size);

    MPI_Barrier(comm);
    start = MPI_Wtime();
    /*
     * read from file
     */
    buf = (unsigned char *)malloc(file_size + 2);
    assert(buf != NULL);
    retval = MPI_File_open(MPI_COMM_SELF, fhname, MPI_MODE_RDONLY, mpi_info, &fh);
    assert(retval == MPI_SUCCESS);
    retval = MPI_File_read(fh, buf, file_size, MPI_CHAR, &status);
    assert(retval == MPI_SUCCESS);
    MPI_File_close(&fh);

    /*
     * uncompress
     */
    origin_size = (unsigned long long *)buf;
    destlen = *origin_size;
    outbuf = (unsigned char *)malloc(destlen);
    assert(outbuf != NULL);
    props = buf + DATA_OFFSET;
    file_size = file_size - DATA_OFFSET - propsize;
    read_size = (size_t)file_size;
    retval = LzmaUncompress(outbuf, &destlen, buf + DATA_OFFSET + propsize, &read_size, props, propsize);
    if(retval != SZ_OK) {
	error_print(retval);
	free(buf);
	free(outbuf);
	exit(1);
    }
#if 0
    /*
     * write into file
     */
    char *fwname;
    fwname = get_fwname(fhname);
    //printf("%s %d\n", fwname, destlen);
    MPI_File_open(MPI_COMM_SELF, fwname, MPI_MODE_WRONLY | MPI_MODE_CREATE, mpi_info, &fw);
    MPI_File_set_size(fw, destlen);
    MPI_File_write(fw, outbuf, destlen, MPI_CHAR, &status);
    MPI_File_close(&fw);
#endif
    MPI_Barrier(comm);
    end = MPI_Wtime();

    size_t uncmprs_len, input_len;
    double cmprs_ratio;
    MPI_Reduce(&read_size, &input_len, 1, MPI_UNSIGNED_LONG, MPI_SUM, 0, comm);
    MPI_Reduce(&destlen, &uncmprs_len, 1, MPI_UNSIGNED_LONG, MPI_SUM, 0, comm);
    if(0 == mpi_rank) {
	double tm = end - start;
	cmprs_ratio = (double)input_len / uncmprs_len;
	printf("file size: %lu\n", input_len);
	printf("after uncompressed: %lu\n", uncmprs_len);
	printf("compress ratio: %f\n", cmprs_ratio);
	printf("number of processes: %d\n", mpi_size);
	printf("time used: %fs\n", tm);
	printf("read speed: %.2fMB/s\n", uncmprs_len / (tm * 1000000));
    }
    MPI_Finalize();
    free(buf);
    free(outbuf);
    free(fhname);
    return 0;
}

char *get_fhname(char *name) {
    char *tmp;
    tmp = malloc(NAME_SIZE);
    sprintf(tmp, "%s_%d_lzma", name, mpi_rank); 

    return tmp;
}

char *get_fwname(char *name) {
    char *tmp;
    tmp = malloc(NAME_SIZE);
    sprintf(tmp, "%s_tmp", name); 

    return tmp;
}

void error_print(int err_code) {
    switch(err_code) {
	case SZ_ERROR_DATA:
	    fprintf(stderr, "Data error!\n");
	    break;
	case SZ_ERROR_MEM:
	    fprintf(stderr, "Memory allocation error!\n");
	    break;
	case SZ_ERROR_UNSUPPORTED:
	    fprintf(stderr, "Unsupported properties!\n");
	    break;
	case SZ_ERROR_INPUT_EOF:
	    fprintf(stderr, "it need more bytes in input buffer(src)!\n");
	    break;
	default:
	    fprintf(stderr, "unknown error!\n");
	    break;
    }
}
