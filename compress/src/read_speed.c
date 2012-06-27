/***********************************************************************
 * Filename : read_speed.c
 * Create : chen cheng 2011-09-06
 * Created Time: 2011-09-06
 * Description: 
 * Modified   : 
 * **********************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <assert.h>

int main(int argc, char **argv) {
    if (argc < 3) {
	printf("Usage: read_speed infile outfile\n");
	exit(1);
    }

    FILE *fp, *fw;
    size_t size, bytes_read;
    unsigned char *buf;
    clock_t tp1, tp2;

    tp1 = clock();  /* time point 1 */
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
    bytes_read = fread(buf, 1, size, fp);
    assert(bytes_read == size);
    fclose(fp);

    tp2 = clock();  /* time point 2 */

    /*
     * write to file
     */
    fw = fopen(argv[2], "wb");
    assert(fw != NULL);
    fwrite(buf, 1, size, fw);
    fclose(fw);

    printf("read time used: %.3f\n", (double)(tp2 - tp1) / CLOCKS_PER_SEC);
    printf("read speed: %.2fMB/s\n", (double)size * CLOCKS_PER_SEC / ((tp2 - tp1) * 1000 * 1000));
    printf("buf[0]: %f\n", *((float *)buf));

    return 0;
}

