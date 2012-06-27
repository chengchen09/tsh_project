/***********************************************************************
 * Filename : mpi_compress.c
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

#define MIN(a, b) (a)<(b)?(a):(b)
#define NAME_SIZE 1024
#define DATA_OFFSET 16

int mpi_size;
int mpi_rank;

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
	MPI_Offset file_size, frag_size, read_size;
	MPI_Offset offset;
	MPI_Status status;
	int retval;
	double start, end;

	unsigned char *buf, *outbuf, *outProps;
	size_t destlen;
	size_t propsize = 5;

	MPI_Init(&argc, &argv);
	MPI_Comm_rank(comm, &mpi_rank);
	MPI_Comm_size(comm, &mpi_size);

	MPI_Barrier(comm);
	start = MPI_Wtime();
	/*
	 * read
	 */
	MPI_File_open(comm, argv[1], MPI_MODE_RDONLY, mpi_info, &fh);
	MPI_File_get_size(fh, &file_size);
	//printf("file size:%d\n", file_size);

	frag_size = file_size / mpi_size;
	offset = frag_size * mpi_rank;
	read_size = MIN(frag_size, file_size - offset);
	//printf("rank %d offset %d\n", mpi_rank, offset);

	buf = malloc(frag_size + 2);
	assert(buf != NULL);
	MPI_File_open(comm, argv[1], MPI_MODE_RDONLY, mpi_info, &fh);
	MPI_File_read_at(fh, offset, buf, read_size, MPI_CHAR, &status);
	MPI_File_close(&fh);

	/*
	 * compress
	 */
	destlen = 1.2 * frag_size + 1024 * 1024;
	outbuf = (unsigned char *)malloc(destlen);
	assert(outbuf != NULL);
	destlen = destlen - DATA_OFFSET -propsize;
	outProps = outbuf + DATA_OFFSET;
	retval = LzmaCompress(outbuf + DATA_OFFSET + propsize, &destlen, buf, read_size, outProps, &propsize, -1, 0, -1, -1, -1, -1, 1);
	if(retval != SZ_OK) {
		error_print(retval);
		free(buf);
		free(outbuf);
		exit(1);
	}

	/*
	 * write
	 */
	char *fwname;
	unsigned long long *len;
	fwname = get_fwname(argv[1]);
	len = (unsigned long long *)outbuf;
	*len = read_size;
	//printf("%s %d\n", fwname, destlen);
	MPI_File_open(MPI_COMM_SELF, fwname, MPI_MODE_WRONLY | MPI_MODE_CREATE, mpi_info, &fw);
	MPI_File_set_size(fw, destlen);
	MPI_File_write(fw, outbuf, destlen + DATA_OFFSET + propsize, MPI_CHAR, &status);
	MPI_File_close(&fw);

	MPI_Barrier(comm);
	end = MPI_Wtime();

	size_t cmprs_len;
	double cmprs_ratio;
	MPI_Reduce(&destlen, &cmprs_len, 1, MPI_UNSIGNED_LONG, MPI_SUM, 0, comm);
	if(0 == mpi_rank) {
		cmprs_ratio = (double)cmprs_len / file_size;
		printf("file size: %lu\n", file_size);
		printf("after compressed: %lu\n", cmprs_len);
		printf("compress ratio: %f\n", cmprs_ratio);
		printf("number of processes: %d\n", mpi_size);
		printf("time used: %fs\n", end - start);
	}
	MPI_Finalize();
	free(fwname);
	free(buf);
	free(outbuf);
	return 0;
}

char *get_fwname(char *name) {
	char *tmp;
	tmp = malloc(NAME_SIZE);
	sprintf(tmp, "%s_%d_lzma", name, mpi_rank); 

	return tmp;
}

void error_print(int err_code) {
	switch(err_code) {
		case SZ_ERROR_PARAM:
			fprintf(stderr, "Incorrect paramater!\n");
			break;
		case SZ_ERROR_MEM:
			fprintf(stderr, "Memory allocation error!\n");
			break;
		case SZ_ERROR_THREAD:
			fprintf(stderr, "errors in multithread functions(only at Mt version)!\n");
			break;
		case SZ_ERROR_OUTPUT_EOF:
			fprintf(stderr, "Output buffer overflow!\n");
			break;
		default:
			fprintf(stderr, "unknown error!\n");
			break;
	}
}
