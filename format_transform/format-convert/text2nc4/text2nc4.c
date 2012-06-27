/***********************************************************************
* Filename : text2nc4.c
* Create : Chen Cheng 2011-04-19
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

long get_fline() {
	int i;
	long max = 0;

	for(i = 0; i < nvars; i++) {
		if(max < vdat_list[i].nelmts)
			max = vdat_list[i].nelmts;
	}
	return max;
}


void set_stride() {
	int i, j, m;
	long max = 0;
	int max_pos;

	/*
	 * find max dimension variable
	 */
	for(i = 0; i < nvars; i++) {
		if(max < vdat_list[i].nelmts) {
			max = vdat_list[i].nelmts;
			max_pos = i;
		}
		vdat_list[i].stride = 1;
	}

	/*
	 * search variables which correspond the max varialbe's dimension
	 * then change their stride
	 * stride = multiple flowing dimensions' length
	 */
	for(i = 0; i < vdat_list[max_pos].ndims; i++) {
		for(j = 0; j < nvars; j++) {
			if(!(strcmp(vdat_list[max_pos].dim_names[i], var_list[j].name))) {
				for(m = i + 1; m < vdat_list[max_pos].ndims; m++) {
					vdat_list[j].stride *= vdat_list[max_pos].dim_lens[m];
				}
			}
		}
	}
}


void read_text(char *path) {
	long fsize;
	size_t read_size;
	char *fbuf;
	FILE *fp;
	long i, j;
	long fline;
	char *newptr, *endptr;
	double tmp;
	int *stride_cnt;
	long *data_cnt;

	
	fp = fopen(path, "r");
	if(fp == NULL) {
		fprintf(stderr, "text file: %s can't be opend\n", path);
		exit(1);
	}
	fseek(fp, 0, SEEK_END);
	fsize = ftell(fp);
	fseek(fp, 0, SEEK_SET);
	fbuf = (char *)malloc(fsize + PAGESIZE);
	read_size = fread(fbuf, 1, fsize, fp);
	fclose(fp);
	
	fline = get_fline();
	set_stride();
	
	stride_cnt = (int *)malloc(nvars * sizeof(int));
	data_cnt = (long *)malloc(nvars * sizeof(long));
	for(i = 0; i < nvars; i++) {
		stride_cnt[i] = 1;
		data_cnt[i] = 0;
	}

	newptr = fbuf;
	for(i = 0; i < fline; i++) {
		for(j = 0; j < nvars; j++) {
			tmp = strtod(newptr, &endptr);
			if(stride_cnt[j] == vdat_list[j].stride && data_cnt[j] < vdat_list[j].nelmts) {	
				if(NC_SHORT == vdat_list[j].type)
					vdat_list[j].sht_data[data_cnt[j]] = (short)tmp;	
				else if(NC_USHORT == vdat_list[j].type)
					vdat_list[j].usht_data[data_cnt[j]] = (unsigned short)tmp;
				else if(NC_INT == vdat_list[j].type)
					vdat_list[j].int_data[data_cnt[j]] = (int)tmp;	
				else if(NC_UINT == vdat_list[j].type)
					vdat_list[j].uint_data[data_cnt[j]] = (unsigned int)tmp;	
				else if(NC_INT64 == vdat_list[j].type)
					vdat_list[j].i64_data[data_cnt[j]] = (long long)tmp;	
				else if(NC_UINT64 == vdat_list[j].type)
					vdat_list[j].ui64_data[data_cnt[j]] = (unsigned long long)tmp;
				else if(NC_FLOAT == vdat_list[j].type)
					vdat_list[j].flt_data[data_cnt[j]] = (float)tmp;	
				else if(NC_DOUBLE == vdat_list[j].type)
					vdat_list[j].dbl_data[data_cnt[j]] = tmp;	
				stride_cnt[j] = 1;
				data_cnt[j]++;
			}
			else
				stride_cnt[j]++;

			newptr = endptr;	
		}
	}
	
	free(fbuf);
	return;
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

	free(count);
	free(start);
	free(vdat_p->data);
}


int main(int argc, char **argv) {
	if(argc < 4) {
		fprintf(stderr, "Usage: text2nc4 xml-infile text-infile nc-outfile\n");
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
	
		read_text(argv[2]);
	
		int i;
		for(i = 0; i < nvars; i++) {
			write_nc_data(ncfile_id, &vdat_list[i]);
		}
		NC_ASSERT(nc_close(ncfile_id));
	}
	MPI_Finalize();
	return 0;
}

