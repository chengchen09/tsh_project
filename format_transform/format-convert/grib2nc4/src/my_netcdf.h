#ifndef _MY_NETCDF_H_
#define _MY_NETCDF_H_

#include "my_defs.h"

#define MAX(a, b) ((a) > (b) ? (a) : (b))

typedef struct _g2nc_4D_info_t{
	int type;
	char * sname;
	char * lname;
	char * units;
	float scale;      /* multiply on it, example: 0.01 to change Pa to gPa or mb */
} g2nc_4D_info_t;

// define a struct to store local info across iterations...
typedef struct _nc_into_t{
	int initialized;
	int ncid;
	int time_dim;
	int time_var;
	int y_dim;
	int x_dim;
	int lev_dim;
	int lev_var;
	char *ncfile;       /* name of file */
	int time_ind;       /* max written to netcdf time step index, 0,1,...*/
	double verf_utime;  /* last written to file verftime value */
	int grads_compatible;
	double time_step;   /* used to check that step is const with '-nc_grads_compatible' option */
	int time_step_type; /* 0 for 'auto' and 1 for 'user' command-line defined value */
	double date0;       /* possible initial or relative user-defined date */
	int date0_type;     /* zero for automatic; 1 for absolute, -1 for relative (alignment only), depends from the command line option only! */
	//    int file_append;   /* is real 'appending' going or it is first call and file is created? */
	int lev_type;      /* only one type allowed to be 4D data */
	int lev_ind;       /* max written to netcdf z-dim index, 0,1,...*/
	int lev_step;      /* to check that levels are going monotonically, save sign only */
	int nc_nlev;       /* max value of z-dim as when file was created */
	int nc_pack;       /* using specified packing */
	float nc_pack_scale, nc_pack_offset; /* packing options */
	float nc_valid_min, nc_valid_max;    /* test and packing options, secondary */
	//g2nc_table *nc_table; /* user-defined conversion table */
	//int free_nc_table;       /* as same table could be shared by differnt instances of netcdf - does free memory here? */
	int nx;
	int ny;
	int nid;           /* total number of fields written or updated in the netcdf file */
	int dim_latlon;    /* lat and lon coordinates dimension, 1 for COARDS or 2 for CF-1.0 conversion */
	int grid_template; /* need for not-mixing in one netcdf file of different grids with same nx,ny: excotic, but...*/
	int nc4;           /* write NetCDF4 version file */
	int endianness;    /* choice for NetCDF4 version file, 0,1,2 - native,little,big */
} nc_info_t;

char * my_get_unixdate(double utime, char * date_str);
void print_error(const char *fmt, const char *string);
void netcdf_func(int status);
int put_nc_global_attr(int ncid, int dim_latlon, int grid_template);
int finalize_nc_info(nc_info_t *local);
//int convert_nc_info(unsigned char **sec, float *data, unsigned int ndata, char *inv_out, nc_info_t **local);
void define_nc_dims(int ncid, int dim_latlon, int *time_dim, int *x_dim, size_t x_len,
			int *y_dim, size_t y_len);
void define_nc_dim_var(int ncid, int dim_latlon, int *time_dim, int *time_var, int *x_dim, 
			int *y_dim, double dx, double dy,
			double verf_utime, double ref_utime, double time_step, int time_step_type,
			double date0, int time_ind, double *lat_data, double *lon_data);
//void put_nc_dim_var();
#endif
