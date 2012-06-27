/***********************************************************************
* Filename : rotate_2d.cpp
* Create : Chen Cheng 2011-08-24
* Description: 
* Modified   : 
* Revision of last commit: $Rev$ 
* Author of last commit  : $Author$ $date$
* licence :
$licence$
* **********************************************************************/

#include <stdlib.h>
#include <assert.h>
#include <stdio.h>
#include "matrixRotation.h"

using namespace matrix;
using namespace std;

void usage()
{
	printf("rotate_2D x y infile outfile\n");
}

int main(int argc, char **argv)
{
	if (argc < 5) {
		usage();
		exit(1);
	}

	size_t dimlens[2], nitems, num;
	float *data;
	float const *rotated_data;
	Matrix2D<float>  *m;
	FILE *fr, *fw;

	dimlens[0] = atoi(argv[1]);
	dimlens[1] = atoi(argv[2]);
	nitems = dimlens[0] * dimlens[1];
	data = new float[nitems];
	assert(data != NULL);

	/* read data from infile */
	fr = fopen(argv[3], "rb");
	assert(fr != NULL);
	num = fread(data, 4, nitems, fr);
	assert(num == nitems);
	fclose(fr);

	/* rotation */
	m = new Matrix2D<float>(dimlens, data);
	m->matrix_rotation();

	/* write data form outfile */
	rotated_data = m->get_rotated_data();
	fw = fopen(argv[4], "wb");
	assert(fw != NULL);
	num = fwrite(rotated_data, 4, nitems, fw);
	assert(num == nitems);
	fclose(fw);

	delete m;
	delete []data;
	return 0;

}
