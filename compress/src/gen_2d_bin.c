/***********************************************************************
 * Filename : gen_bin.c
 * Create : chen 2011-10-24
 * Created Time: 2011-10-24
 * Description: 
 * Modified   : 
 * **********************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>


typedef int dtype_t;

const int dtype_size = 4;
int X_FACTOR = 10;

int main(int argc, char **argv) {

    if (argc < 4) {
	printf("Usage: gen_bin x y outfile x_factor(optional)\n");
	return 1;
    }

    int x, y;
    int i, j;
    dtype_t *data; 

    x = atoi(argv[1]);
    y = atoi(argv[2]);
    assert(x > 0 && y > 0);

    /* initial data */
    data = (dtype_t *)malloc(x * y * dtype_size);
    assert(data != NULL);

    for (i = 0; i < x; i++)
	for (j = 0; j < y; j++)
	    data[i * y + j] = i * X_FACTOR + j;


    /* write binary data */
    FILE *fp;
    fp = fopen(argv[3], "wb");
    assert(fp != NULL);
    fwrite(data, dtype_size, x * y, fp);
    fclose(fp);

    return 0;
}

