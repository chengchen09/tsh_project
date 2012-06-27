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
int X_FACTOR = 100;
int Y_FACTOR = 10;

int main(int argc, char **argv) {

    if (argc < 5) {
	printf("Usage: gen_bin x y z outfile x_factor(optional)\n");
	return 1;
    }

    if (argc > 5)
	X_FACTOR = atoi(argv[5]);

    int x, y, z;
    int i, j, k;
    dtype_t *data; 

    x = atoi(argv[1]);
    y = atoi(argv[2]);
    z = atoi(argv[3]);
    assert(x > 0 && y > 0 && z > 0);

    /* initial data */
    data = (dtype_t *)malloc(x * y * z * dtype_size);
    assert(data != NULL);

    for (i = 0; i < x; i++)
	for (j = 0; j < y; j++)
	    for (k = 0; k < z; k++)
		data[i * y * z + j * z + k] = i * X_FACTOR + j * Y_FACTOR + k;


    /* write binary data */
    FILE *fp;
    fp = fopen(argv[4], "wb");
    assert(fp != NULL);
    fwrite(data, dtype_size, x * y * z, fp);
    fclose(fp);

    return 0;
}

