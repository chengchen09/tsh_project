/***********************************************************************
* Filename : extract.c
* Create : Chen Cheng 2011-06-03
* Description: 
* Modified   : 
* Revision of last commit: $Rev$ 
* Author of last commit  : $Author$ $date$
* licence :
$licence$
* **********************************************************************/

#include <stdlib.h>
#include <stdio.h>
#include <assert.h>
#include <string.h>

#define NAME_LEN 1024

int main(int argc, char **argv) {
	
	if(argc < 3) { 
		printf("Usage: extrace infile 64 m\n");
		exit(1);
	}
	
	FILE *fp, *fw;
	size_t base = 1;
	size_t size;
	int pos = 2;
	unsigned char *buf;
	char fname[NAME_LEN];

	if(argc >= 4) {
		if((strcmp(argv[3], "k") == 0) || (strcmp(argv[3], "K") == 0)) {
			base = 1024;
			pos = 1;
		}
		else if((strcmp(argv[3], "m") == 0) || (strcmp(argv[3], "M") == 0)) {
			base = 1024 * 1024;
			pos = 0;
		}
	}
	
	size = atoi(argv[2]) * base;
	buf = (unsigned char *)malloc(size);
	assert(buf != NULL);
	
	/*
	 * read
	 */
	fp = fopen(argv[1], "r");
	assert(fp != NULL);
	fread(buf, 1, size, fp);
	fclose(fp);

	/*
	 * write
	 */
	char suffix[3][2] = {"M", "K", "B"};
	sprintf(fname, "%s_%s%s", argv[1], argv[2], suffix[pos]); 
	fw = fopen(fname, "w");
	assert(fw != NULL);
	fwrite(buf, 1, size, fw);
	fclose(fw);

	return 0;
}
