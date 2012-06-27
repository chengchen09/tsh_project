/***********************************************************************
* Filename : gen_bin.c
* Create : Chen Cheng 2011-04-22
* Description: 
* Modified   : 
* Revision of last commit: $Rev$ 
* Author of last commit  : $Author$ $date$
* licence :
$licence$
* **********************************************************************/

#include <stdio.h>
#include <stdlib.h>

#define X 4
#define Y 4
#define Z 4

typedef float dtype;

int main(int argc, char **argv) {
	int i; 
	dtype *data;
	FILE *fh;

	size_t nelmts = X * Y;

	fh = fopen("exmp.bin", "wb");
	if(fh == NULL) {
		fprintf(stderr, "file open error\n");
		exit(1);
	}

	data = (dtype *)malloc(nelmts * sizeof(dtype));
	if(!data) {
		fprintf(stderr, "memory allocate error\n");
		exit(1);
	}

	for(i = 0; i < nelmts; i++) {
		data[i] = 1;
	}

	fwrite(data, sizeof(dtype), nelmts, fh);

	fclose(fh);

	return 0;
}
