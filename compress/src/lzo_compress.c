/***********************************************************************
* Filename : lzo_compress.c
* Create : Chen Cheng 2011-09-05
* Description: 
* Modified   : 
* Revision of last commit: $Rev$ 
* Author of last commit  : $Author$ $date$
* licence :
$licence$
* **********************************************************************/

#include "lzo/lzoconf.h"
#include "lzo/lzo1x.h"
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <time.h>

#define NAME_LEN 1024
#define DATA_OFFSET 8


int main(int argc, char **argv) {
	if(argc < 2) {
		printf("Usage: lzo_compress infile\n");
		exit(1);
	}

	FILE *fp;
	FILE *fw;
	unsigned char *buf, *outbuf;
	char *wrkmem;
	char fname[NAME_LEN];
	size_t size, destlen;
	size_t bytes_read;
	clock_t start, end;
	unsigned long long *original_len;
	int status;

	/*
	 * initialize lzo library
	 */
	if (lzo_init() != LZO_E_OK) {
	    printf("internal error - lzo_init() failed !!!\n");
	    printf("(this usually indicates a compiler bug - try recompiling\nwithout optimizations, and enable '-DLZO_DEBUG' for diagnostics)\n");
	    return 4;
	}

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
	destlen = size + size / 16 + 64 + 3 + DATA_OFFSET;
	outbuf = (unsigned char *)malloc(destlen);
	wrkmem = (char *)malloc(LZO1X_1_MEM_COMPRESS);
	assert(outbuf != NULL && wrkmem != NULL);
	destlen = destlen - DATA_OFFSET;
	original_len = (unsigned long long *)outbuf;
	*original_len = size;
	start = clock();    /* time start */
	status = lzo1x_1_compress(buf, size, outbuf + DATA_OFFSET, &destlen, wrkmem);
	if(status != LZO_E_OK) {
	    /* this should NEVER happen */
	    printf("internal error - compression failed: %d\n", status);
	    free(buf);
	    free(outbuf);
	    exit(1);
	}
	end = clock();	/* time end */

	/*
	 * write into file
	 */
	/*len = (unsigned long long *)outbuf;
	*len = size;*/
	sprintf(fname, "%s_lzo", argv[1]);	
	fw = fopen(fname, "w");
	fwrite(outbuf, 1, destlen + DATA_OFFSET, fw);
	
	printf("file size: %lu\n", size);
	printf("after compressed: %lu\n", destlen);
	printf("compression ratio: %.2f%\n", (double)destlen * 100 / size);
	printf("time used: %.3fs\n", (double)(end - start) / CLOCKS_PER_SEC);
	fclose(fw);
	free(buf);
	free(outbuf);
	return 0;	
}

