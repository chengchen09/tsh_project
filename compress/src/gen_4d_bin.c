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
int X_FACTOR = 1000;
int Y_FACTOR = 100;
int Z_FACTOR = 10;

int main(int argc, char **argv) {

    if (argc < 6) {
	printf("Usage: gen_bin x y z t outfile\n");
	return 1;
    }

    int x, y, z, t;
    int i, j, k, l;
    dtype_t *data; 

    x = atoi(argv[1]);
    y = atoi(argv[2]);
    z = atoi(argv[3]);
    t = atoi(argv[4]);
    assert(x > 0 && y > 0 && z > 0 && t > 0);

    /* initial data */
    data = (dtype_t *)malloc(x * y * z * t * dtype_size);
    assert(data != NULL);

    for (i = 0; i < x; i++)
	for (j = 0; j < y; j++)
	    for (k = 0; k < z; k++)
		for (l = 0; l < t; l++)
		    data[i * y * z * t + j * z *t + k * t + l] = i * X_FACTOR + j * Y_FACTOR + k * Z_FACTOR + l;


    /* write binary data */
    FILE *fp;
    fp = fopen(argv[5], "wb");
    assert(fp != NULL);
    fwrite(data, dtype_size, x * y * z * t, fp);
    fclose(fp);

    return 0;
}

