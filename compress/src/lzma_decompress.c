/***********************************************************************
 * Filename : lzma_decompress.c
 * Create : Chen Cheng 2011-06-07
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

#define DATA_OFFSET 16 

void error_print(int err_code);

int main(int argc, char **argv) {
    if(argc < 3) {
	printf("Usage: lzma_decompress infile outfile\n");
	exit(1);
    }

    FILE *fp;
    FILE *fw;
    unsigned char *buf, *outbuf, *props;
    size_t size, destlen;
    size_t propsize = LZMA_PROPS_SIZE;
    size_t bytes_read;
    clock_t tp1, tp2, tp3;
    int status;
    unsigned long long *origin_size;

    /* get file size */
    fp = fopen(argv[1], "rb");
    assert(fp != NULL);
    fseek(fp, 0, SEEK_END);
    size = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    /*
     * read from file
     */
    buf = (unsigned char *)malloc(size + 2);
    assert(buf != NULL);
    tp1 = clock();
    bytes_read = fread(buf, 1, size, fp);
    assert(bytes_read == size);
    fclose(fp);

    /*
     * uncompress
     */
    origin_size = (unsigned long long *)buf;
    destlen = *origin_size;
    outbuf = (unsigned char *)malloc(destlen);
    assert(outbuf != NULL);
    props = buf + DATA_OFFSET;
    size = size - DATA_OFFSET - propsize;
    tp2 = clock();    /* time start */
    status = LzmaUncompress(outbuf, &destlen, buf + DATA_OFFSET + propsize, &size, props, propsize);
    if(status != SZ_OK) {
	error_print(status);
	free(buf);
	free(outbuf);
	exit(1);
    }
    tp3 = clock(); /* time end */

    /*
     * write into file
     */
    fw = fopen(argv[2], "w");
    fwrite(outbuf, 1, destlen, fw);

    printf("file size: %lu\n", size);
    printf("after uncompressed: %lu\n", destlen);
    printf("compression ratio: %.2f%\n", (double)size * 100 / destlen);
    printf("decompression time used: %.3fs\n", (double)(tp3 - tp2) / CLOCKS_PER_SEC);
    printf("read time used: %.3f\n", (double)(tp2 - tp1) / CLOCKS_PER_SEC);
    printf("IO speed: %.2fMB/s\n", (double)destlen * CLOCKS_PER_SEC / ((tp3 - tp1) * 1000 * 1000));
    fclose(fw);
    free(buf);
    free(outbuf);
    return 0;	
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
