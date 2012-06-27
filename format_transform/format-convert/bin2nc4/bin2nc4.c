/***********************************************************************
* Filename : bin2nc4.c
* Create : Chen Cheng 2011-04-21
* Description: 
* Modified   : 
* Revision of last commit: $Rev$ 
* Author of last commit  : $Author$ $date$
* licence :
$licence$
* **********************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <netcdf.h>
#include <mpi.h>

#include "xml_parser.h"
#include "support.h"

#define DIM_NAME_LEN 50
#define PAGESIZE 4096

#define NC_ASSERT(status) do { \
	if((status) != NC_NOERR) { \
		fprintf(stderr, "Line %d: %s\n", __LINE__, strerror(status)); \
		exit(1); \
	} \
}while(0)
	
#define MEMORY_CHECK(ptr) do { \
	if(ptr == NULL){ \
		fprintf(stderr, "line %d: memory allocate error!\n", __LINE__); \
		exit(1); \
	} \
}while(0)

struct att_xml_t att_list[ATT_MAX_NUM];
struct var_xml_t var_list[VAR_MAX_NUM];
struct dim_xml_t dim_list[DIM_MAX_NUM];
struct var_dat_t vdat_list[VAR_MAX_NUM];
int natts = 0;
int nvars = 0;
int ndims = 0;
int byteorder = LSBFIRST;
int mpi_rank;
int mpi_size;

void get_var_type(struct var_xml_t *var_p, struct var_dat_t *vdata_p) {
	if((strcmp(var_p->type, "int 2")) == 0) {
		vdata_p->type = NC_SHORT;
		vdata_p->sht_data = malloc(vdata_p->nelmts * 2);
		vdata_p->data = vdata_p->sht_data;
	}
	else if((strcmp(var_p->type, "int 4")) == 0) {
		vdata_p->type = NC_INT;
		vdata_p->int_data = malloc(vdata_p->nelmts * 4);
		vdata_p->data = vdata_p->int_data;
	}
	else if((strcmp(var_p->type, "int 8")) == 0) {
		vdata_p->type = NC_INT64;
		vdata_p->i64_data = malloc(vdata_p->nelmts * 8);
		vdata_p->data = vdata_p->i64_data;
	}
	else if((strcmp(var_p->type, "uint 2")) == 0) {
		vdata_p->type = NC_USHORT;
		vdata_p->usht_data = malloc(vdata_p->nelmts * 2);
		vdata_p->data = vdata_p->usht_data;
	}
	else if((strcmp(var_p->type, "uint 4")) == 0) {
		vdata_p->type = NC_UINT;
		vdata_p->uint_data = malloc(vdata_p->nelmts * 4);
		vdata_p->data = vdata_p->uint_data;
	}
	else if((strcmp(var_p->type, "uint 8")) == 0) {
		vdata_p->type = NC_UINT64;
		vdata_p->ui64_data = malloc(vdata_p->nelmts * 8);
		vdata_p->data = vdata_p->ui64_data;
	}
	else if((strcmp(var_p->type, "float 4")) == 0) {
		vdata_p->type = NC_FLOAT;
		vdata_p->flt_data = malloc(vdata_p->nelmts * 4);
		vdata_p->data = vdata_p->flt_data;
	}
	else if((strcmp(var_p->type, "float 8")) == 0) {
		vdata_p->type = NC_DOUBLE;
		vdata_p->dbl_data = malloc(vdata_p->nelmts * 8);
		vdata_p->data = vdata_p->dbl_data;
	}
	else {
		fprintf(stderr, "type defined is not supported in variable %s\n", var_p->name);
		exit(1);
	}
	MEMORY_CHECK(vdata_p->data);

	return;
}

void get_var_shape(struct var_xml_t *var_p, struct var_dat_t *vdata_p) {
	char *sep = " ";
	char *shape, *token;
	int i = 0;

	shape = (char *)malloc((strlen(var_p->shape) + 1) * sizeof(char));
	if(shape == NULL) {
		fprintf(stderr, "LINE %d: memory allocate error\n", __LINE__);
		exit(1);
	}
	strcpy(shape, var_p->shape);
	
	token = strtok(shape, sep);
	while(token){
		strcpy(&(vdata_p->dim_names[i][0]), token);
		i++;
		token = strtok(NULL, sep);
	}

	vdata_p->ndims = i;
	free(shape);

}

void write_nc_header(int grp_id) {
	int i, j, status;
	int *dimids, *var_dimids;
	size_t len;

	/*
	 * define global attributes
	 */
	for(i = 0; i < natts; i++) {
		len = strlen(att_list[i].value);
		status = nc_put_att_text(grp_id, NC_GLOBAL, att_list[i].name, len + 1, att_list[i].value);
		NC_ASSERT(status);
	}

	/*
	 * define dimensions
	 */
	dimids = (int *)malloc(ndims * sizeof(int));
	if(dimids == NULL){
		fprintf(stderr, "line %d: memory allocate error!\n", __LINE__);
		exit(1);
	}
	for(i = 0; i < ndims; i++) {
		status = nc_def_dim(grp_id, dim_list[i].name, atoi(dim_list[i].length), &(dimids[i]));
		NC_ASSERT(status);
	}

	/* 
	 * define variables
	 */
	var_dimids = (int *)malloc(ndims * sizeof(int));
	MEMORY_CHECK(var_dimids);
	
	for(i = 0; i < nvars; i++) {
		/* dimension lens */
		vdat_list[i].dim_lens = (size_t *) malloc(vdat_list[i].ndims * sizeof(size_t));
		MEMORY_CHECK(vdat_list[i].dim_lens);
	
		/* dimension names */
		vdat_list[i].dim_names = (char **)malloc(ndims * sizeof(char *));
		MEMORY_CHECK(vdat_list[i].dim_names);
		for(j = 0; j < ndims; j++) {
			vdat_list[i].dim_names[j] = (char *)malloc(DIM_NAME_LEN * sizeof(char));
			MEMORY_CHECK(vdat_list[i].dim_names[j]);
		}
	}

	for(i = 0; i < nvars; i++) {
		get_var_shape(&var_list[i], &vdat_list[i]);
		
		vdat_list[i].nelmts = 1;
		for(j = 0; j < vdat_list[i].ndims; j++) {
			status = nc_inq_dimid(grp_id, vdat_list[i].dim_names[j], &var_dimids[j]);
			NC_ASSERT(status);
			int tmp_len; 
			status = nc_inq_dimlen(grp_id, var_dimids[j], &tmp_len);
			vdat_list[i].dim_lens[j] = tmp_len;
			vdat_list[i].nelmts *= tmp_len;
		}
		
		get_var_type(&var_list[i], &vdat_list[i]);
		
		status = nc_def_var(grp_id, var_list[i].name, vdat_list[i].type, vdat_list[i].ndims, var_dimids, &vdat_list[i].varid);
		NC_ASSERT(status);

		/* define varialbe attributes */
		for(j = 0; j < var_list[i].natts; j++) {
			len = strlen(var_list[i].att_list[j].value);
			status = nc_put_att_text(grp_id, vdat_list[i].varid, var_list[i].att_list[j].name, len + 1, var_list[i].att_list[j].value);
			NC_ASSERT(status);
		}
	}

	/*
	 * free memory
	 */
	free(var_dimids);
	free(dimids);
}



int get_local_btd() {
	int btd = -1;
	union
	{
		unsigned short s;
		unsigned char c[sizeof(unsigned short)];
	} un;
	un.s = 0x0201;
	if(2 == sizeof(unsigned short)) {
		if((2 == un.c[0]) && (1 == un.c[1]))
			btd = MSBFIRST;
		else if((1 == un.c[0]) && (2 == un.c[1]))
			btd = LSBFIRST;
		else {
			fprintf(stderr, "byte order unknow\n");
			exit(1);
		}
	}
	else {
		fprintf(stderr, "sizeof(unsigned short) = %u\n", (unsigned int)sizeof(unsigned short));
		exit(1);
	}

	return btd;
} 


void read_bin(char *path) {
	int local_btd;
	int i;
	MPI_File fp;
	MPI_Status status;
	
	MPI_File_open(MPI_COMM_SELF, path, MPI_MODE_RDONLY, MPI_INFO_NULL, &fp);

	local_btd = get_local_btd();
	
	/* no need to convert byte order */
	if(local_btd == byteorder) {
		for(i = 0; i < nvars; i++) {
			if(NC_SHORT == vdat_list[i].type)
				MPI_File_read(fp, vdat_list[i].sht_data, vdat_list[i].nelmts, MPI_SHORT, &status);
			else if(NC_USHORT == vdat_list[i].type)
				MPI_File_read(fp, vdat_list[i].usht_data, vdat_list[i].nelmts, MPI_UNSIGNED_SHORT, &status);
			else if(NC_INT == vdat_list[i].type)
				MPI_File_read(fp, vdat_list[i].int_data, vdat_list[i].nelmts, MPI_INT, &status);
			else if(NC_UINT == vdat_list[i].type)
				MPI_File_read(fp, vdat_list[i].uint_data, vdat_list[i].nelmts, MPI_UNSIGNED, &status);
			else if(NC_INT64 == vdat_list[i].type)
				MPI_File_read(fp, vdat_list[i].i64_data, vdat_list[i].nelmts, MPI_LONG_LONG, &status);
			else if(NC_UINT64 == vdat_list[i].type)
				MPI_File_read(fp, vdat_list[i].ui64_data, vdat_list[i].nelmts, MPI_UNSIGNED_LONG_LONG, &status);
			else if(NC_FLOAT == vdat_list[i].type)
				MPI_File_read(fp, vdat_list[i].flt_data, vdat_list[i].nelmts, MPI_FLOAT, &status);
			else if(NC_DOUBLE == vdat_list[i].type)
				MPI_File_read(fp, vdat_list[i].dbl_data, vdat_list[i].nelmts, MPI_DOUBLE, &status);
		}
	}
	/* convert the byte order */
	else if(LSBFIRST == local_btd || MSBFIRST == local_btd) {
		short sh;
		int it;
		long long ll;
		unsigned short ush;
		unsigned int uit;
		unsigned long long ull;
		unsigned long j;
		for(i = 0; i < nvars; i++) {
			if(NC_FLOAT == vdat_list[i].type)
				MPI_File_read(fp, vdat_list[i].flt_data, vdat_list[i].nelmts, MPI_FLOAT, &status);
			else if(NC_DOUBLE == vdat_list[i].type)
				MPI_File_read(fp, vdat_list[i].dbl_data, vdat_list[i].nelmts, MPI_DOUBLE, &status);
			else {
				for(j = 0; j < vdat_list[i].nelmts; j++) {
					if(NC_SHORT == vdat_list[i].type) {
						MPI_File_read(fp, &sh, 1, MPI_SHORT, &status);
						vdat_list[i].sht_data[j] = swap16(sh);
					}
					else if(NC_USHORT == vdat_list[i].type) {
						MPI_File_read(fp, &ush, 1, MPI_UNSIGNED_SHORT, &status);
						vdat_list[i].usht_data[j] = swap16(ush);
					}
					else if(NC_INT == vdat_list[i].type) {
						MPI_File_read(fp, &it, 1, MPI_INT, &status);
						vdat_list[i].int_data[j] = swap32(it);
					}
					else if(NC_UINT == vdat_list[i].type) {
						MPI_File_read(fp, &uit, 1, MPI_UNSIGNED, &status);
						vdat_list[i].uint_data[j] = swap32(uit);
					}
					else if(NC_INT64 == vdat_list[i].type) {
						MPI_File_read(fp, &ll, 1, MPI_LONG_LONG, &status);
						vdat_list[i].i64_data[j] = swap64(ll);
					}
					else if(NC_UINT64 == vdat_list[i].type) {
						MPI_File_read(fp, &ull, 1, MPI_UNSIGNED_LONG_LONG, &status);
						vdat_list[i].ui64_data[j] = swap64(ull);
					}
				}
			}
		}
	}
	else {
		fprintf(stderr, "can't get local byteorder\n");
		exit(1);
	}

}


void write_nc_data(int pa_ncid, struct var_dat_t *vdat_p) {
	int status;
	size_t *start, *count; 
	
	start = (size_t *)malloc(vdat_p->ndims * sizeof(size_t));
	count = (size_t *)malloc(vdat_p->ndims * sizeof(size_t));
	MEMORY_CHECK(start);
	MEMORY_CHECK(count);

	set_hyperslab(count, start, vdat_p->dim_lens, vdat_p->ndims, 1, mpi_rank);
	nc_var_par_access(pa_ncid, vdat_p->varid, NC_INDEPENDENT);
	status = nc_put_vara(pa_ncid, vdat_p->varid, start, count, vdat_p->data);
	NC_ASSERT(status);

	free(start);
	free(count);
	free(vdat_p->data);
}


int main(int argc, char **argv) {
	if(argc < 4) {
		fprintf(stderr, "Usage: bin2nc4 xml-infile bin-infile nc-outfile\n");
		exit(1);
	}

	int ncfile_id, grp_id;
	int status;
	long fsize;
	size_t read_size;
	char *fbuf;
	FILE *fp;

	MPI_Comm comm = MPI_COMM_WORLD;
	MPI_Info mpi_info = MPI_INFO_NULL;

	MPI_Init(&argc, &argv);
	MPI_Comm_rank(comm, &mpi_rank);
	MPI_Comm_size(comm, &mpi_size);

	//TODO: multiple processes write the variables
	if(mpi_rank == 0) {
		parser_xml_file(argv[1]);
	
		status = nc_create_par(argv[3], NC_NETCDF4 | NC_MPIIO, MPI_COMM_SELF, mpi_info, &ncfile_id); 
		NC_ASSERT(status);
		
		write_nc_header(ncfile_id);
		
		read_bin(argv[2]);
	
		int i;
		for(i = 0; i < nvars; i++) {
			write_nc_data(ncfile_id, &vdat_list[i]);
		}
		NC_ASSERT(nc_close(ncfile_id));
	}
	MPI_Finalize();
	return 0;
}

