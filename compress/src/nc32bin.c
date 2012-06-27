/***********************************************************************
* Filename : nc32bin.c
* Create : Chen Cheng 2011-06-22
* Description: 
* Modified   : 
* Revision of last commit: $Rev$ 
* Author of last commit  : $Author$ $date$
* licence :
$licence$
* **********************************************************************/
#include <stdio.h>
#include <netcdf.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>

#define DIM_LEN 5

#define NC_ASSERT(status) do { \
	if((status) != NC_NOERR) { \
		fprintf(stderr, "Line %d: %s\n", __LINE__, nc_strerror(status)); \
		exit(1); \
	} \
}while(0)
	
#define MEMORY_CHECK(ptr) do { \
	if(ptr == NULL){ \
		fprintf(stderr, "line %d: memory allocate error!\n", __LINE__); \
		exit(1); \
	} \
}while(0)


size_t get_type_size(int type) {
	size_t size = -1;
	
	switch(type) {
		case NC_SHORT:
		case NC_USHORT:
			size = 2;
			break;
		
		case NC_INT:
		case NC_UINT:
			size = 4;
			break;
		
		case NC_INT64:
		case NC_UINT64:
			size = 8;
			break;
		
		case NC_FLOAT:
			size = 4;
			break;
		
		case NC_DOUBLE:
			size = 8;
			break;
		
		default:
			break;
	}
	return size;

}


int main(int argc, char **argv) {
	if(argc < 4) {
		printf("Usage: nc32bin varname nc3file binfile\n");
		exit(1);
	}

	int status, i, ndims;
	int ncfile_id, ncvar_id;
	int dimids[DIM_LEN];
	size_t size, nelmts, buflen;
	size_t dimlens[DIM_LEN];
	nc_type vartype;
	void *buf = NULL;

	/*
	 * read nc file
	 */
	status = nc_open(argv[2], NC_NOWRITE, &ncfile_id);
	NC_ASSERT(status);

	status = nc_inq_varid(ncfile_id, argv[1], &ncvar_id);
	NC_ASSERT(status);
	
	nc_inq_varndims(ncfile_id, ncvar_id, &ndims);
	nc_inq_vartype(ncfile_id, ncvar_id, &vartype);
	nc_inq_vardimid(ncfile_id, ncvar_id, dimids);
	for(i = 0; i < ndims; i++)
		nc_inq_dimlen(ncfile_id, dimids[i], &(dimlens[i]));

	size = get_type_size(vartype);
	nelmts = 1;
	for(i = 0; i < ndims; i++)
		nelmts *= dimlens[i];
	buflen = size * nelmts;
	buf = malloc(buflen);
	MEMORY_CHECK(buf);
	
	if(NC_SHORT == vartype)
		status = nc_get_var_short(ncfile_id, ncvar_id, (short *)buf);
	else if(NC_USHORT == vartype)
		status = nc_get_var_ushort(ncfile_id, ncvar_id, (unsigned short *)buf);
	else if(NC_INT == vartype)
		status = nc_get_var_int(ncfile_id, ncvar_id, (int *)buf);
	else if(NC_UINT == vartype)
		status = nc_get_var_uint(ncfile_id, ncvar_id, (unsigned int *)buf);
	else if(NC_INT64 == vartype)
		status = nc_get_var_longlong(ncfile_id, ncvar_id, (long long *)buf);
	else if(NC_UINT64 == vartype)
		status = nc_get_var_ulonglong(ncfile_id, ncvar_id, (unsigned long long *)buf);
	else if(NC_FLOAT == vartype)
		status = nc_get_var_float(ncfile_id, ncvar_id, (float *)buf);
	else if(NC_DOUBLE == vartype)
		status = nc_get_var_double(ncfile_id, ncvar_id, (double *)buf);
	
	NC_ASSERT(status);
	nc_close(ncfile_id);

	
	/*
	 * write binary
	 */
	FILE *fp = NULL;
	fp = fopen(argv[3], "wb");
	assert(fp != NULL);
	fwrite(buf, size, nelmts, fp);
	fclose(fp);

	return 0;
}

