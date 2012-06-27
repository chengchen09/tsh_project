/***********************************************************************
 * Filename : lzo_uncompress.c
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

#define DATA_OFFSET 8 


int main(int argc, char **argv) {
    if(argc < 3) {
	printf("Usage: lzo_decompress infile outfile\n");
	exit(1);
    }

    FILE *fp;
    FILE *fw;
    unsigned char *buf, *outbuf;
    size_t size, destlen;
    size_t bytes_read;
    clock_t tp1, tp2, tp3;
    int status;
    unsigned long long *origin_size;

    tp1 = clock(); /* time point 1 */
    /* get file size */
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
     * uncompress
     */
    origin_size = (unsigned long long *)buf;
    destlen = *origin_size;
    outbuf = (unsigned char *)malloc(destlen);
    assert(outbuf != NULL);
    size = size - DATA_OFFSET;
    tp2 = clock();    /* time point 2 */
    status = lzo1x_decompress(buf + DATA_OFFSET, size, outbuf, &destlen, NULL); 
    if(status != LZO_E_OK || destlen != *origin_size) {
        /* this should NEVER happen */
        printf("internal error - decompression failed: %d\n", status);
	free(buf);
	free(outbuf);
	exit(1);
    }
    tp3 = clock(); /* time point 3 */

    /*
     * write into file
     */
    fw = fopen(argv[2], "w");
    fwrite(outbuf, 1, destlen, fw);
    fclose(fw);

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

