/***********************************************************************
* Filename : lzma_compress.c
* Create : Chen Cheng 2011-06-03
* Description: 
* Modified   : 
* Revision of last commit: $Rev$ 
* Author of last commit  : $Author$ $date$
* licence :
$licence$
* **********************************************************************/

#include "LzmaLib.h"
#include "Types.h"
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <time.h>

#define NAME_LEN 1024
#define DATA_OFFSET 16 

void error_print(int err_code);

int main(int argc, char **argv) {
	if(argc < 2) {
		printf("Usage: lzma_compress infile\n");
		exit(1);
	}

	FILE *fp;
	FILE *fw;
	unsigned char *buf, *outbuf, *outProps;
	char fname[NAME_LEN];
	size_t size, destlen;
	size_t propsize = LZMA_PROPS_SIZE;
	size_t bytes_read;
	clock_t start, end;
	unsigned long long *len;
	int status = SZ_OK;

	/*
	 * get file size
	 */
	fp = fopen(argv[1], "r");
	assert(fp != NULL);
	fseek(fp, 0, SEEK_END);
	size = ftell(fp);
	fseek(fp, 0, SEEK_SET);

	/*
	 * read from file
	 */
	buf = (unsigned char *)malloc(size + 2);
	assert(buf != NULL);
	bytes_read = fread(buf, 1, size, fp);
	assert(bytes_read == size);
	fclose(fp);

	/*
	 * compress
	 */
	destlen = 1.2 * size + 1024 * 1024;
	outbuf = (unsigned char *)malloc(destlen);
	assert(outbuf != NULL);
	destlen = destlen - DATA_OFFSET - propsize;
	outProps = outbuf + DATA_OFFSET;
	start = clock();    /* time start */
	status = LzmaCompress(outbuf + DATA_OFFSET + propsize, &destlen, buf, size, outProps, &propsize, -1, 0, -1, -1, -1, -1, 1);
	if(status != SZ_OK) {
		error_print(status);
		free(buf);
		free(outbuf);
		exit(1);
	}
	end = clock();	/* time end */
		
	/*
	 * write into file
	 */
	len = (unsigned long long *)outbuf;
	*len = size;
	sprintf(fname, "%s_lzma", argv[1]);	
	fw = fopen(fname, "w");
	fwrite(outbuf, 1, destlen + DATA_OFFSET + propsize, fw);
	
	printf("file size: %lu\n", size);
	printf("after compressed: %lu\n", destlen);
	printf("compression ratio: %.2f%\n", (double)destlen * 100 / size);
	printf("time used: %.3fs\n", (double)(end - start) / CLOCKS_PER_SEC);
	fclose(fw);
	free(buf);
	free(outbuf);
	return 0;	
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
