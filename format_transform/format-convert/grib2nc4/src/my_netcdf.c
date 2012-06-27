#include <stdio.h>
#include <netcdf.h>
#include <string.h>
#include <math.h>
#include <time.h>
#include <stdlib.h>

#include "my_netcdf.h"
#include "my_parallel.h"
//#include "wgrib2nc.h"
//#include "netcdf_sup

char * my_get_unixdate(double utime, char * date_str)
{
  struct tm *gmt;
  time_t gmt_t;

  gmt_t=(time_t)utime;
  gmt = gmtime(&gmt_t);
  sprintf(date_str,"%.4d.%.2d.%.2d %.2d:%.2d:%.2d UTC",
  gmt->tm_year+1900,gmt->tm_mon+1,gmt->tm_mday,gmt->tm_hour,gmt->tm_min,gmt->tm_sec);
  return date_str;
}

void print_error(const char *fmt, const char *string)
{
    fprintf(stderr, "\n*** FATAL ERROR: ");
    fprintf(stderr, fmt, string);
    fprintf(stderr," ***\n\n");
    exit(8);
    return;
}

void netcdf_func(int status)
{
  if (status != NC_NOERR) print_error("netcdf error %s", nc_strerror(status));
}

int finalize_nc_info(nc_info_t *save)
{

#ifdef DEBUG_NC
	fprintf(stderr,"netcdf: were added/updated %d fields, file %s\n",save->nid,save->ncfile);
#endif
   
    if (save->ncid >=0)  
	  nc_close(save->ncid);
    free(save->ncfile);

    free(save);
    return 0;
}


int put_nc_global_attr(int ncid, int dim_latlon, int grid_template)
{
    char *str;
    str="";
      if (dim_latlon == 1)
        str = "COARDS";
      else if (dim_latlon == 2)
        str = "CF-1.0";
      else
        fatal_error("netcdf:create_nc_dims: %s","unsupported lat-lon dimension");

      netcdf_func( nc_put_att_text(ncid, NC_GLOBAL, "Conventions", strlen(str), str) );

      netcdf_func( nc_put_att_int (ncid, NC_GLOBAL, "GRIB2_grid_template",
                                NC_INT, 1, &grid_template) );
}


void define_nc_dims(int ncid, int dim_latlon, int *time_dim, int *x_dim, size_t x_len,
                    int *y_dim, size_t y_len)
{
    char *name;
// create coordinate variables...
  if (dim_latlon == 1)
  {
    netcdf_func( nc_def_dim (ncid, "latitude", y_len, y_dim) );
    netcdf_func( nc_def_dim (ncid, "longitude", x_len, x_dim) );
  }
  else if(dim_latlon == 2)
  {
    netcdf_func( nc_def_dim (ncid, "y", y_len, y_dim) );
    netcdf_func( nc_def_dim (ncid, "x", x_len, x_dim) );
  }
  else
    print_error("netcdf:create_nc_dims: %s","unsupported lat-lon dimension");

   netcdf_func( nc_def_dim (ncid, "time", NC_UNLIMITED, time_dim) );
   
}

void define_nc_dim_var(int ncid, int dim_latlon, int *time_dim, int *time_var, int *x_dim, 
						int *y_dim, double dx, double dy,
                        double verf_utime, double ref_utime, double time_step, int time_step_type,
			    double date0, int time_ind, double *lat_data, double *lon_data)
{
     char *name, *str, *lname, *units;
     char ref_date[50];
     int dimids[2];
     int lat_var, lon_var, y_var, x_var;
     int ref_time_type;
     double fill_value = FILL_VALUE_DOUBLE;
     float  f_fill_value = FILL_VALUE_FLOAT;

     /* the variable need by put_nc_dim_var */
     double ttime;
     int i, j;
     double *test_ll;
     size_t start[2], count[2];
     size_t x_len, y_len;
     size_t dimlens[2];
     
// create coordinate variables...
     if (dim_latlon == 1)
    {

    dimids[0] = *y_dim;
    netcdf_func( nc_def_var (ncid, "latitude", NC_DOUBLE, 1, dimids, &lat_var) );
    dimids[0] = *x_dim;
    netcdf_func( nc_def_var (ncid, "longitude", NC_DOUBLE, 1, dimids, &lon_var) );
/*#ifdef DEBUG_NC
fprintf(stderr,"netcdf:create_nc_dims: latitude(dim,var,ny)=%d %d %d\n",*y_dim,lat_var,ny);
fprintf(stderr,"netcdf:create_nc_dims: longitude(dim,var,nx)=%d %d %d\n",*x_dim,lon_var,nx);
#endif*/
  }
  else if(dim_latlon == 2)
  {

    dimids[0] = *y_dim;
    netcdf_func( nc_def_var (ncid, "y", NC_DOUBLE, 1, dimids, &y_var) );
    str = "y coordinate of projection";
//    str = "projection_y_coordinate";    //it is standard_name,
                                        //but could require grid_mapping definition that is
                                        //projection-dependant; could do it later;
                                        //do not need lat-lon values in this case as these would be
                                        //computed in processing software (CF convention)
    netcdf_func( nc_put_att_text(ncid, y_var, "long_name", strlen(str), str) );
    str = "projection_y_coordinate";
    netcdf_func( nc_put_att_text(ncid, y_var, "standard_name", strlen(str), str) );
    dimids[0] = *x_dim;
    netcdf_func( nc_def_var (ncid, "x", NC_DOUBLE, 1, dimids, &x_var) );
    str = "x coordinate of projection";
    netcdf_func( nc_put_att_text(ncid, x_var, "long_name", strlen(str), str) );
    str = "projection_x_coordinate";
    netcdf_func( nc_put_att_text(ncid, x_var, "standard_name", strlen(str), str) );
    str = "m";
    netcdf_func( nc_put_att_text(ncid, y_var, "units", strlen(str), str) );
    netcdf_func( nc_put_att_text(ncid, x_var, "units", strlen(str), str) );
    netcdf_func( nc_put_att_double(ncid, x_var, "grid_spacing", NC_DOUBLE, 1, &dx) );
    netcdf_func( nc_put_att_double(ncid, y_var, "grid_spacing", NC_DOUBLE, 1, &dy) );
    dimids[0] = *y_dim;
    dimids[1] = *x_dim;
    netcdf_func( nc_def_var (ncid, "latitude", NC_DOUBLE, 2, dimids, &lat_var) );
    netcdf_func( nc_def_var (ncid, "longitude", NC_DOUBLE, 2, dimids, &lon_var) );
/*#ifdef DEBUG_NC
fprintf(stderr,"netcdf:create_nc_dims: y(dim,var,ny)=%d %d %d\n",*y_dim,y_var,ny);
fprintf(stderr,"netcdf:create_nc_dims: x(dim,var,nx)=%d %d %d\n",*x_dim,x_var,nx);
fprintf(stderr,"netcdf:create_nc_var (2D): latitude(xdim,ydim,var)=%d %d %d\n",*y_dim,*x_dim,lat_var);
fprintf(stderr,"netcdf:create_nc_var (2D): longitude(xdim,ydim,var)=%d %d %d\n",*y_dim,*x_dim,lon_var);
#endif*/
  }
  else
    print_error("netcdf:create_nc_dims: %s","unsupported lat-lon dimension");

  str = "degrees_north";
  netcdf_func( nc_put_att_text(ncid, lat_var, "units", strlen(str), str) );
  str = "degrees_east";
  netcdf_func( nc_put_att_text(ncid, lon_var, "units", strlen(str), str) );
  str = "latitude";
  netcdf_func( nc_put_att_text(ncid, lat_var, "long_name", strlen(str), str) );
  str = "longitude";
  netcdf_func( nc_put_att_text(ncid, lon_var, "long_name", strlen(str), str) );

  /* time settings */
  dimids[0] = *time_dim;
  netcdf_func(nc_def_var (ncid, "time", NC_DOUBLE, 1, dimids, time_var));
  netcdf_func(nc_put_att_double(ncid, *time_var, "_FillValue", NC_DOUBLE, 1, &fill_value));
  str = "seconds since 1970-01-01 00:00:00.0 0:00";
  netcdf_func( nc_put_att_text(ncid, *time_var, "units", strlen(str), str) );
  //str = "verification time generated by function verftime()";
  //netcdf_func( nc_put_att_text(ncid, *time_var, "long_name", strlen(str), str) );

  /* nc_ref_time is reference time value,
     nc_ref_time_type is the reference time type:
     0 - undefined
     1 - analyses, all for the same reference date, could be succeded by forecasts
     2 - analyses, but for different reference date/time (time serie)
     3 - forecasts from the same reference date/time
     For the type 0 or 2 nc_ref_time keeps first field reference date/time
  */
  if (fabs(ref_utime - verf_utime) < TM_TOLERANCE )
  {
    ref_time_type = 1;
    str = "analyses, reference date is fixed";
  }
  else
  {
    ref_time_type = 3;
    str = "forecast or accumulated, reference date is fixed";
  }
  netcdf_func( nc_put_att_double(ncid, *time_var, "reference_time", NC_DOUBLE, 1, &ref_utime) );
  netcdf_func( nc_put_att_int(ncid, *time_var,    "reference_time_type", NC_INT, 1, &ref_time_type) );
  my_get_unixdate(ref_utime, ref_date);
  netcdf_func( nc_put_att_text(ncid, *time_var,   "reference_date", strlen(ref_date), ref_date) );
  netcdf_func( nc_put_att_text(ncid, *time_var,   "reference_time_description", strlen(str), str) );
  /* write time step attributes, could be -1 (undefined) for first or single time step, then update */
  if ( time_step_type )
  {
    str="user";
  }
  else
  {
    str="auto";
  }
  netcdf_func( nc_put_att_text(ncid, *time_var, "time_step_setting", strlen(str), str) );
  netcdf_func( nc_put_att_double(ncid, *time_var,  "time_step", NC_DOUBLE, 1, &time_step) );

#ifdef DEBUG_NC
fprintf(stderr,"netcdf:create_nc_dims: time(dim,var)=%d %d, size unlimited\n",*time_dim,*time_var);
fprintf(stderr,"netcdf:create_nc_dims: time_step=%.1lf time_step_type=%d (%s)\n",
time_step,time_step_type,str);
#endif
  
     netcdf_func( nc_enddef(ncid) );


/*
  * put var
  */
     // populate coordinate variables...
  start[0] = 0;
  netcdf_func( nc_put_var1_double(ncid, *time_var, start, &date0) );

  if ( time_ind == 1 )
  {
    start[0] = 1;
    netcdf_func( nc_put_var1_double(ncid, *time_var, start, &verf_utime) );
  }
  else if (time_ind > 1)
  {
    for (j=1; j <= time_ind; j++)
    {
      start[0] = j;
      ttime = date0 + (time_step)*j;
      netcdf_func( nc_put_var1_double(ncid, *time_var, start, &ttime) );
    }
  }
  else if (time_ind < 0)
  {
   fatal_error("netcdf:create_nc_dims: %s","negative time index");
  }

    netcdf_func(nc_inq_dimlen(ncid, *x_dim, &x_len));
    netcdf_func(nc_inq_dimlen(ncid, *y_dim, &y_len));
    
  i = MAX(x_len, y_len);
  test_ll = (double*) malloc(i*sizeof(double));
  if (!test_ll) fatal_error("netcdf:create_nc_dims: %s",
    "error doing malloc of test_ll");
  
  if (dim_latlon == 1)
  {
        set_hyperslab(count, start, &y_len, 1);
        for (i=0; i < count[0] ; i++) 
            test_ll[i] = lat_data[(i+start[0])*x_len];
        //start[0] = 0; count[0] = y_len;
        netcdf_func( nc_put_vara_double(ncid, lat_var, start, count, test_ll) );

        set_hyperslab(count, start, &x_len, 1);
        for (i=0; i<count[0]; i++) 
            test_ll[i] = lon_data[i+start[0]];
        //start[0] = 0; count[0] = x_len;
        netcdf_func( nc_put_vara_double(ncid, lon_var, start, count, test_ll) );
  }
  else
  {
        set_hyperslab(count, start, &y_len, 1);
        for (i=0; i<count[0] ; i++) 
            test_ll[i] = dy*(i+start[0]);
        //start[0] = 0; count[0] = y_len;
        netcdf_func( nc_put_vara_double(ncid, y_var, start, count, test_ll) );

        set_hyperslab(count, start, &x_len, 1);
        for (i=0; i<count[0]; i++) 
            test_ll[i] = dx*(i+start[0]);
        //start[0] = 0; count[0] = x_len;
        netcdf_func( nc_put_vara_double(ncid, x_var, start, count, test_ll) );

        //start[0] = 0;  count[0] = y_len;
        //start[1] = 0;  count[1] = x_len;

        dimlens[0] = y_len;
        dimlens[1] = x_len;
        set_hyperslab(count, start, dimlens, 2);
        netcdf_func( nc_put_vara_double(ncid, lat_var, start, count, &(lat_data[start[0] * x_len + start[1]])) );
        netcdf_func( nc_put_vara_double(ncid, lon_var, start, count, &(lon_data[start[0] * x_len + start[1]])) );
  }
  free(test_ll);
}
