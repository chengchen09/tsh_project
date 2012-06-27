#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <math.h>
//#include <time.h>
#include <ctype.h>
#include "grb2.h"
#include "wgrib2.h"
#include "fnlist.h"
#include "wgrib2nc.h"

#include <assert.h>
#include <mpi.h>
#include "my_netcdf.h"
#include "my_convert.h"

//#define DEBUG_NC


//#if defined USE_NETCDF3 || defined USE_NETCDF4
#if 1
#include <netcdf.h>

/* defined in Netcdf_sup.c */
extern int nc_grads_compatible;
extern int nc_pack;
extern float nc_pack_offset;
extern float nc_pack_scale;
extern float nc_valid_min;
extern float nc_valid_max;
extern int nc_nlev;
extern double nc_date0;
extern int    nc_date0_type;  /* 1 for absolute, -1 for relative (alignment only) */
extern double nc_dt;          /* -1 will be used for variable (undefined) step */
extern int nc4;

/* defined in my_convert.c */
extern int file_append;
extern int decode, latlon;
extern double *lat, *lon;
extern int nx, ny;
// extern unsigned in npnts;
extern enum output_order_type output_order;
extern int mode, new_GDS;


static int check_nc_latlon( int ncid, int y_dim, int x_dim, int dim_latlon );

static int get_nc_time_ind(int ncid, double verf_utime, int time_var, int time_dim,
                  int * time_ind, double * last_verf_time,
                  double * tm_step, int time_step_type, double date0 );
static int update_nc_ref_time(int ncid, double verf_utime, double ref_utime,
                       int time_var, int time_step_type, double time_step);



/******************************************************************************************/
static int g2nc_type2pack(int type)
{
  switch (type)
  {
    case NC_BYTE:
      return G2NC_PACK_BYTE;
    case NC_SHORT:
      return G2NC_PACK_SHORT;
    case NC_FLOAT:
      return G2NC_PACK_FLOAT;
    case NC_DOUBLE:
      return G2NC_PACK_DOUBLE;
    default:
      return G2NC_PACK_UNDEF;
  }
}
static int g2nc_pack2type(int pack)
{
  switch (pack)
  {
    case G2NC_PACK_BYTE:
      return NC_BYTE;
    case G2NC_PACK_SHORT:
      return NC_SHORT;
    case G2NC_PACK_FLOAT:
      return NC_FLOAT;
    case G2NC_PACK_DOUBLE:
      return NC_DOUBLE;
    case G2NC_PACK_UNDEF:  /*default - float*/
      return NC_FLOAT;
    default:
      return NC_NAT;
  }
}
/*_FillValue for different data types, could be used for packing (short,byte), max value*/
static short       sfill_value = G2NC_FILL_VALUE_SHORT;
static signed char bfill_value = G2NC_FILL_VALUE_BYTE;
static float       ffill_value = G2NC_FILL_VALUE_FLOAT;
static double      dfill_value = G2NC_FILL_VALUE_DOUBLE;

/****************
 * cheap copy of f_netcdf
 ****************/
 int convert_nc_info(unsigned char **sec, float *data, unsigned int ndata,
                                    char *inv_out, nc_info_t **local)
{
  
    nc_info_t *save;

  int varid, i, ok;
  unsigned int j;
  char varname_buf[_MAX_PATH],level_buf[_MAX_PATH];
  char name[_MAX_PATH], desc[_MAX_PATH], unit[_MAX_PATH];
  int time_ind;      /* current time step index to write in netcdf */
  int level_ind=-1;   /* current z-dim index to write in netcdf */
  size_t start[4];   /* record start location in 4D = {TIME0, LEV0, LAT0, LON0}; */
  size_t count[4];   /* record size in 4D = {TIMES, LEVS, LATS, LONS}; */
  int dimids[4],test_dimids[4];   //lev_types[2];

  size_t chunks[4];
  int shuffle, deflate, deflate_level;

  int year, month, day, hour, minute, second, err_code;
  int grid_template, ndims;
  double verf_utime, tm_step, dx, dy, ref_utime;
  int level_type1 = -1, level_type2 = -1;
  int undef_val1, undef_val2;
//  int center, subcenter;
  float level_val1=0, level_val2, dz, dz1; //level_val2=0,lev_vals[2], test_val1, test_val2;
  float scale_factor, add_offset, valid_min, valid_max;
  double max=0, min=0, range=0;
  nc_type var_type;
  int var_pack;
  float* fdata;
  short* sdata;
  signed char* bdata;
  char *str;
  float dd;
  unsigned char *gds;

  size_t dimlens[2];
  size_t bufsize, nvar;
  int offset;
  int rank;

  rank = MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    assert(rank >= 0);

    //if(rank == 0){      /* start of if(rank == 0) */
        
    // mode 0,1,2 for decoding a message.
    if (output_order != wesn && output_order != wens) fatal_error("netcdf: only works in we:sn order","");
    save = *local;
    if (new_GDS && save->initialized && save->nid ) fatal_error("netcdf: only one grid type at a time","");

    // Specify dimension of lat-lon coordinates (1 - COARDS, 2 - CF-1.0)
    dx = 1;
    dy = 1;

    /*
     * deal with grid template
     */
    grid_template = code_table_3_1(sec);
    
    if (save->initialized)
    {
      if (save->grid_template != grid_template)
      {
        fatal_error("netcdf: grid templates differs from this in netcd file %s",save->ncfile);
      }
    }
    else
    {
      if (grid_template == 0 || grid_template == 10 || grid_template == 40)
      {
        // COARDS convention only allows lat-lon, mercator and gaussian
        save->dim_latlon = 1; //"COARDS", rectangular non-rotated grids
      }
      else if (grid_template == 20 || grid_template == 30)
      {
        // CF convention allows much more...
        save->dim_latlon = 2; //"CF-1.0"; include here also rotated grids when becomes possible
        gds = sec[3];
        if (grid_template == 20)
        {
          dy = fabs(GDS_Polar_dy(gds));
          dx = fabs(GDS_Polar_dx(gds));
//      lov = GDS_Polar_lov(gds);
//      lad = GDS_Polar_lad(gds);
        }
        else if (grid_template == 30)
        {
          dy = fabs(GDS_Lambert_dy(gds));
          dx = fabs(GDS_Lambert_dx(gds));
//      lov = GDS_Lambert_Lov(gds);
//      lad = GDS_Lambert_Lad(gds); //etc.
        }
      }
      else {
        fprintf(stderr,"netcdf: doesn't support yet grid_template %d\n",grid_template);
        // fatal_error_i("netcdf: doesn't support grid_template %d",grid_template);
        save->dim_latlon = 2; //"CF-1.0"; include here also rotated grids when becomes possible
      }
      save->grid_template=grid_template;
    }
    /* end for grid template */
    
    if (save->dim_latlon > 1 && save->grads_compatible)
      fatal_error_i("netcdf: non-COARDS netCDF is not GrADS v1.9b4 compatible,\n"
                    "        unsupported grid_template=%d,\n"
                    "        but possibly could be open by GrADS with valid ctl (data description) file",
                    grid_template);

    // To avoid problems in future: used occasionally are ndata (main), npnts, nx and ny (extern).
    // For the eligible grid_templates ndata==npnts==nx*ny; does it needs to check it once more?

    /*
    What variable, levels we got?
    f_lev for some templates return 0 (OK) and do not write nothing to level_buf
    */
    level_buf[0]=0;
    
    //TODO:data, mode
    //err_code = f_lev(mode, sec, data, ndata, level_buf, local);
    err_code = f_lev(0, sec, data, ndata, level_buf, NULL);
//    if (strcmp(level_buf, "reserved")==0) return(0);
    if (err_code || strlen(level_buf)==0 || strcmp(level_buf, "reserved")==0)
      fatal_error("netcdf: undefined or unsupported (reserved?) level: %s",level_buf);

	//TODO:mode
    //ok=getName(sec, mode, NULL, name, desc, unit);
    ok=getName(sec, 0, NULL, name, desc, unit);
	

#ifdef DEBUG_NC
fprintf(stderr,"netcdf: Start processing of %s at %s\n", name, level_buf);
#endif

    if ( save->nc_nlev ) ndims = 4;  /* suppose 4D shape */
    else                 ndims = 3;  /* 3D shape */

    ndims = 3;

    /* First check the 'ignore' keyword in the user defined vc (var conversion directives).
     * Look for the "exact" matching of name and level
     * in the user defined conversion table, then for 'any' level (*) */


    /*
     * deal with time
     */
    err_code = verftime(sec, &year, &month, &day, &hour, &minute, &second);
    if(err_code) fatal_error("netcdf: can not get verftime","");

    verf_utime = get_unixtime(year, month, day, hour, minute, second, &err_code);
    if(err_code) fatal_error("netcdf: time [sec] is out of range for this OS","");

    i = reftime(sec, &year, &month, &day, &hour, &minute, &second);
    ref_utime = get_unixtime(year, month, day, hour, minute, second, &err_code);
    if (err_code) fatal_error("netcdf: time out of range for presentation in sec on this system","");

    time_ind=-1;
    if (verf_utime+G2NC_TM_TOLERANCE < save->date0 )
    { /* time is before date0 */
      if (save->date0_type > 0 )   return 0; /* skip silently */
      else if (save->initialized ) fatal_error("netcdf: input time before initial in file %s",save->ncfile);
    }
    if (save->time_step_type &&                   /* time_step is user def and */
       (save->date0_type || save->initialized))   /* date0 is already defined */
    {
      time_ind = (int)((verf_utime - save->date0)/save->time_step+G2NC_TM_TOLERANCE);
      if (fabs(save->date0+save->time_step*time_ind-verf_utime) > G2NC_TM_TOLERANCE)
      {
//        if (save->date0_type ) { /*not auto type, +-1*/

           return 0; /* non-integer time index, skip silently */
//        }
//        else fatal_error("netcdf: input time do not time-aligned to initial and time step in file %s",save->ncfile);
      }

      
      /* Check for too large gap in data */
      if (( save->initialized && abs(save->time_ind-time_ind) > G2NC_TM_IND_GAP ) ||
          (!save->initialized && save->date0_type > 0 && time_ind > G2NC_TM_IND_GAP ))
      {
        fatal_error("netcdf: too large gap in time index %s",save->ncfile);
      }
    }
    /* end with time deal */

    if ( !save->initialized )
    {
      save->nx = nx;
      save->ny = ny;
    }
    
    /* Do next check for each new field added to the netcdf file */
    if (nx != save->nx || ny != save->ny )
    {
      fprintf(stderr,"netcdf: error processing %s at %s\n", name, level_buf);
      fprintf(stderr,"netcdf: existing grid (nx*ny) %d x %d, new grid %d x %d\n",
       save->nx,save->ny,nx,ny);
      fatal_error("netcdf: grid size must be same for all variables","");
    }
    
    /* work around z-dim:
     * is this data eligible for treating as 4D?
     * which level it is etc. Used code from Level.c */

    fixed_surfaces(sec,&level_type1,&level_val1,&undef_val1,&level_type2,&level_val2,&undef_val2);

#ifdef DEBUG_NC
fprintf(stderr,"netcdf: ndims=%d, levt1=%d, levt2=%d level_val1*scale=%g\n",
ndims,level_type1,level_type2, level_val1);
#endif


      /* code to make variable names legal */
      varname_buf[0] = 0;
      if (isdigit(name[0])) strcpy(varname_buf, "N");  // no names starting with digit
      strcat(varname_buf, name);

      if ( ndims == 3 )
      {
        strcat(varname_buf, "_");
        strcat(varname_buf, level_buf);
      }
      delchars(varname_buf, ' ');
      rep_chars(varname_buf, '-','_');
      rep_chars(varname_buf, '.','_');
      rep_chars(varname_buf, '+','_');
      /* added more characters wne 5/09 */
      rep_chars(varname_buf, '=','_');
      rep_chars(varname_buf, '^','_');
      rep_chars(varname_buf, '(','_');
      rep_chars(varname_buf, ')','_');
      rep_chars(varname_buf, '/','_');
 
    //}/* end if (rank == 0) */

      
    if ( !save->initialized )
    {
       // if(rank == 0){      
            /* first record in new file */
            if (save->date0_type <= 0)
            {/* undef or alignment requested (was checked earlier) */
                time_ind = 0; /* first record */
                save->date0 = verf_utime;
            }
            else
            {   
                if (save->time_step_type == 0)
                {/* step undefined (auto), but defined date0 */
                    if (fabs(save->date0-verf_utime) > G2NC_TM_TOLERANCE )
                    { /* first time is not user-defined start time date0 */
                        time_ind = 1; /* pass first and write data as the second record */
                        save->time_step = verf_utime - save->date0;
                    }
                }
                else
                    time_ind = (int)((verf_utime - save->date0)/(save->time_step)+G2NC_TM_TOLERANCE);
              }
             
              //else it was already defined (know date0 and step)
              save->time_ind = time_ind;
              save->verf_utime = verf_utime;
              // define NEW nc file, assume nx and ny of the 1st message hold for all messages...
              // get some info for grid_mapping
       // }/* end if rank == 0 */

        /* TODO: send save */
        /* TODO: send the ncfile */
        //netcdf_func( nc_create(save->ncfile, NC_NETCDF4, &(save->ncid)) );
        netcdf_func(nc_create_par(save->ncfile, NC_NETCDF4 | NC_MPIIO, MPI_COMM_WORLD, MPI_INFO_NULL, &(save->ncid)));
        
        /* TODO: send the dim_latlon */
        put_nc_global_attr(save->ncid, save->dim_latlon, save->grid_template);

        define_nc_dims(save->ncid, save->dim_latlon, &(save->time_dim), &(save->x_dim), 
                    save->nx, &(save->y_dim), save->ny);

        /* send dx, dy, ref_utime, lat, lon */
        define_nc_dim_var(save->ncid, save->dim_latlon, &(save->time_dim), &(save->time_var), &(save->x_dim), 
					&(save->y_dim), dx, dy,
					save->verf_utime, ref_utime, save->time_step, save->time_step_type,
					save->date0, save->time_ind, lat, lon);

      save->initialized = 1;
    }
    else
    { /* check data in open netcdf file */
      tm_step = save->time_step;
      time_ind = get_nc_time_ind(save->ncid, verf_utime,
                    save->time_var, save->time_dim, &(save->time_ind),
                    &(save->verf_utime), &tm_step,
                    save->time_step_type, save->date0 );

      if(time_ind < 0) fatal_error("netcdf: unresolved timing problem","");
      if (save->grads_compatible)
      {
        if( tm_step > G2NC_TM_TOLERANCE )
        {
          if( save->time_step > G2NC_TM_TOLERANCE )
          {
            if (fabs(save->time_step-tm_step) > G2NC_TM_TOLERANCE)
            {
              fatal_error("netcdf: variable time stepping is not GrADS v1.9b4 compatible","");
            }
          }
          else save->time_step = tm_step;
        }
      }

      i = update_nc_ref_time(save->ncid, verf_utime, ref_utime,
                           save->time_var, save->time_step_type, tm_step);
   /*
    * Check does existing lats lons in netcdf file are same
    * as this of new field?
    */
      i = check_nc_latlon(save->ncid, save->y_dim, save->x_dim, save->dim_latlon);
    }


    //netcdf is open or created and defined, data do not rejected, go ahead...

    /* Set chunking, shuffle, and deflate. */
    shuffle = 1;
    deflate = 1;
    deflate_level = 1;

    dimids[0] = save->time_dim;
    chunks[0] = 1;

 
      dimids[1] = save->y_dim;
      dimids[2] = save->x_dim;
      /*chunks[2] = ny;
      chunks[3] = nx;*/
      chunks[1] = save->ny;
      chunks[2] = save->nx;

    ok = nc_inq_varid (save->ncid, varname_buf, &varid);
    var_pack = G2NC_PACK_UNDEF;
    scale_factor = 0.;
    if (ok != NC_NOERR)
    {
      varid = -1; /* new variable, not in file yet */

        if (save->nc_pack)
      {
        var_pack = save->nc_pack;
        scale_factor = save->nc_pack_scale;
        add_offset = save->nc_pack_offset;
        valid_min = save->nc_valid_min;
        valid_max = save->nc_valid_max;
      }
    }
    else
    {  /* same variable found in open netcdf file */
      netcdf_func( nc_inq_vartype (save->ncid, varid, &var_type) );
      /* Check existing variable definition in netcdf with requested */

        if (nc_pack) var_pack = save->nc_pack;
//      if ( var_type == NC_FLOAT ) var_type = 0;  //Jan 2007, vsm
      if ( var_pack != G2NC_PACK_UNDEF && var_type != g2nc_pack2type(var_pack) )
      {
        fatal_error("netcdf: variable %s exists with other packing size",varname_buf);
      }
      netcdf_func( nc_inq_varndims (save->ncid, varid, &i) );
      if ( i != ndims )
      {
        fprintf(stderr,"netcdf: ndims=%d in file, new ndims=%d\n",i,ndims);
        fatal_error("netcdf: variable %s exists having different dimension",varname_buf);
      }
      test_dimids[0]=test_dimids[1]=test_dimids[2]=test_dimids[3]=-1;
      netcdf_func( nc_inq_vardimid (save->ncid, varid, test_dimids) );
      for ( i = 0; i < ndims; i++ )
      {
        if (test_dimids[i] != dimids[i])
        {
          fatal_error("netcdf: variable %s exists with different dimension shape",varname_buf);
        }
      }
      /* packing information is taken from the open netcdf file */
      if ( var_type == NC_BYTE || var_type == NC_SHORT )
      {
        ok = nc_get_att_float(save->ncid, varid, "scale_factor", &scale_factor);
        if (ok == NC_NOERR)
        {
          ok = nc_get_att_float(save->ncid, varid, "add_offset", &add_offset);
          if (ok == NC_NOERR) var_pack = g2nc_type2pack(var_type);
        }
        if (var_pack != g2nc_type2pack(var_type))
          fatal_error("netcdf: wrong or absent packing info for SHORT or BYTE var=%s,\n"
                      "        invalid netCDF file",varname_buf);
      }
      else if (var_type == NC_FLOAT)
      {
        // next attributes COULD present
        ok = nc_get_att_float(save->ncid, varid, "valid_min", &valid_min);
        if (ok == NC_NOERR)
        {
          ok = nc_get_att_float(save->ncid, varid, "valid_max", &valid_max);
          if (ok == NC_NOERR) var_pack = G2NC_PACK_FLOAT; // will check the valid_range
        }
      }
    }
    if (var_pack == G2NC_PACK_BYTE && save->grads_compatible)
      fatal_error("netcdf: byte packing is not GrADS v1.9b4 compatible, var=%s,\n"
                  "        but such netCDF file still could be open by GrADS\n"
                  "        with valid ctl (data description) file", varname_buf);

//    if ( var_pack ) {
      // Find max-min for checking of packing parameters
      // vsm: From Jan 2007 version values that do not fit to provided valid_range
      // are replaced by UNDEFINED
    if ( scale_factor == 0. && (var_pack == G2NC_PACK_BYTE || var_pack == G2NC_PACK_SHORT) )
    {
      max = min = ok = 0;
      //TODO:data
      for (j = 0; j < ndata; j++)
      {
        if (!UNDEFINED_VAL(data[j]))
        {
          if (ok)
          {
            max = max > data[j] ? max : data[j];
            min = min < data[j] ? min : data[j];
          }
          else
          {
            ok = 1;
            max = min = data[j];
          }
        }
      }
    }
    if (varid < 0)
    { /* new variable, not in file yet */

#ifdef DEBUG_NC
fprintf(stderr,"netcdf: add new var=%s to output file %s\n",varname_buf,save->ncfile);
#endif
      netcdf_func( nc_redef(save->ncid) );

      /* try to apply provided packing to all new variables */
      if ( var_pack == G2NC_PACK_BYTE )
      {
        netcdf_func( nc_def_var (save->ncid, varname_buf, NC_BYTE, ndims, dimids, &varid) );
        netcdf_func( nc_put_att_schar(save->ncid, varid, "_FillValue", NC_BYTE, 1, &bfill_value) );
      }
      else if ( var_pack == G2NC_PACK_SHORT )
      {
        netcdf_func( nc_def_var (save->ncid, varname_buf, NC_SHORT, ndims, dimids, &varid) );
        netcdf_func( nc_put_att_short(save->ncid, varid, "_FillValue", NC_SHORT, 1, &sfill_value) );
      }
      else
      { /* default case of unpacked float variables */
        netcdf_func( nc_def_var (save->ncid, varname_buf, NC_FLOAT, ndims, dimids, &varid) );
        netcdf_func( nc_put_att_float(save->ncid, varid, "_FillValue", NC_FLOAT, 1, &ffill_value) );
      }
//vsm: which 'short name' is correct for the NetCDF COARDS compliant file? 'name' or 'varname_buf'?
//      netcdf_func( nc_put_att_text(save->ncid, varid, "short_name", strlen(name), name) );
      netcdf_func( nc_put_att_text(save->ncid, varid, "short_name", strlen(varname_buf), varname_buf) );
      netcdf_func( nc_put_att_text(save->ncid, varid, "long_name", strlen(desc), desc) );
/*
      lev_types[0]=level_type1;
      lev_types[1]=level_type2;
      netcdf_func( nc_put_att_int(save->ncid, varid, "grib2_lt", NC_INT, 2, lev_types) );
*/

      netcdf_func( nc_put_att_text(save->ncid, varid, "level", strlen(level_buf), level_buf) );
 
      fix_units(unit,sizeof(unit));
      netcdf_func( nc_put_att_text(save->ncid, varid, "units", strlen(unit), unit) );

      if (save->dim_latlon > 1 )
      {
        str="longitude latitude";
        netcdf_func( nc_put_att_text(save->ncid, varid, "coordinates", strlen(str), str) );
      }
      if ( var_pack == G2NC_PACK_BYTE || var_pack == G2NC_PACK_SHORT )
      {
        if ( scale_factor == 0.)
        {
          /* Auto-packing, center near 0 as signed char or short are used.
             Try secure scaling for all next input fields: extend the range on +-10%... */
          range = (max-min)*1.2;
          add_offset = (float) (min+max)*0.5;
          if ( var_pack == G2NC_PACK_BYTE )
            scale_factor = (float) (range/(bfill_value - 2))*0.5;
          else if ( var_pack == G2NC_PACK_SHORT )
            scale_factor = (float) (range/(sfill_value - 2))*0.5;
#ifdef DEBUG_NC
fprintf(stderr,"netcdf: auto-packing, min=%lf, max=%lf, offset=%f, scale=%f, var=%s\n",
min, max, add_offset, scale_factor, varname_buf);
#endif
        }
        netcdf_func( nc_put_att_float(save->ncid, varid, "scale_factor", NC_FLOAT, 1, &scale_factor) );
        netcdf_func( nc_put_att_float(save->ncid, varid, "add_offset", NC_FLOAT, 1, &add_offset) );
      }
      else if ( var_pack == G2NC_PACK_FLOAT )
      {
        netcdf_func( nc_put_att_float(save->ncid, varid, "valid_min", NC_FLOAT, 1, &valid_min) );
        netcdf_func( nc_put_att_float(save->ncid, varid, "valid_max", NC_FLOAT, 1, &valid_max) );
      }
      // TODO: add this part will cause the nc file lose the variables
	  //if (save->nc4)
      //{
        //netcdf_func( nc_def_var_chunking(save->ncid, varid, NC_CHUNKED, chunks) );
        //netcdf_func( nc_def_var_deflate( save->ncid, varid, shuffle, deflate, deflate_level) );
//        netcdf_func( nc_var_set_cache(save->ncid, varid, nx*ny);
//        netcdf_func( nc_def_var_endian(save->ncid, varid, endian) );
//        NC_ENDIAN_NATIVE,NC_ENDIAN_LITTLE,NC_ENDIAN_BIG
      //}
      netcdf_func( nc_enddef(save->ncid) );
    }

    // specify where to write new data portion - record size and location in 4D or 3D
    start[0] = time_ind; count[0] = 1;
    /*if ( ndims == 4 )
    {
      start[1] = level_ind; count[1] = 1;
      start[2] = 0;         count[2] = ny;
      start[3] = 0;         count[3] = nx;
    }
    else*/
    {
      start[1] = 0;  count[1] = ny;
      start[2] = 0;  count[2] = nx;
    }
    dimlens[0] = save->ny;
    dimlens[1] = save->nx;

    set_hyperslab(&(count[1]), &(start[1]), dimlens, ndims-1);
    nvar = count[1] * count[2];
    offset = start[1] * save->nx;
      
    // pack data and write one time step
    if ( var_pack == G2NC_PACK_BYTE || var_pack == G2NC_PACK_SHORT )
    { /* get packing attributes from file */
      netcdf_func( nc_get_att_float(save->ncid, varid, "scale_factor", &scale_factor) );
      netcdf_func( nc_get_att_float(save->ncid, varid, "add_offset", &add_offset) );
      if ( fabs(scale_factor) < 1e-20) fatal_error("netcdf: zero packing scale factor %s",varname_buf);

      /*set_hyperslab(&(count[1]), &(start[1]), dimlens, ndims-1);
      nvar = count[1] * count[2];
      offset = start[1] * save->nx;*/
      
      if ( var_pack == G2NC_PACK_BYTE )
      {
        valid_min = -(bfill_value-2)*scale_factor + add_offset;
        valid_max =  (bfill_value-2)*scale_factor + add_offset;
        
        bdata = (signed char*) malloc(nvar*sizeof(signed char));
        if (!bdata) 
            fatal_error("netcdf: %s","error doing malloc of bdata");
        
        for (j = 0; j < nvar; j++)
        {
          bdata[j] = (UNDEFINED_VAL(data[j+offset]) || data[j+offset] < valid_min || data[j+offset] > valid_max ) ?
             bfill_value : (data[j+offset] - add_offset) / scale_factor;
        }
        netcdf_func( nc_put_vara_schar(save->ncid, varid, start, count, bdata) );
        free(bdata);
      }
      else if (var_pack == G2NC_PACK_SHORT )
      {
        valid_min = -(sfill_value-2)*scale_factor + add_offset;
        valid_max =  (sfill_value-2)*scale_factor + add_offset;
        
        sdata = (short*) malloc(nvar*sizeof(short));
        if (!sdata) 
            fatal_error("netcdf: %s","error doing malloc of sdata");
            
        for (j = 0; j < nvar; j++)
        {
          sdata[j] = (UNDEFINED_VAL(data[j+offset]) || data[j+offset] < valid_min || data[j+offset] > valid_max ) ?
            sfill_value : (data[j+offset] - add_offset) / scale_factor;
        }
        netcdf_func( nc_put_vara_short(save->ncid, varid, start, count, sdata) );
        free(sdata);
      }
    }
    else
    { /* not packed float values */
      if (var_pack == G2NC_PACK_FLOAT)
      { // check valid_range
        fdata = (float*) malloc(nvar*sizeof(float));
        if (!fdata) fatal_error("netcdf: %s","error doing malloc of fdata");
        for (j = 0; j < nvar; j++)
        {
          fdata[j] = (UNDEFINED_VAL(data[j+offset]) || data[j+offset] < valid_min || data[j+offset] > valid_max ) ?
            ffill_value : data[j+offset];
        }
        netcdf_func( nc_put_vara_float(save->ncid, varid, start, count, fdata) );
        free(fdata);
      }
      else
      {
        netcdf_func( nc_put_vara_float(save->ncid, varid, start, count, &(data[offset])) );
      }
    }
#ifdef DEBUG_NC
fprintf(stderr,"netcdf: added var=%s to output file %s\n",varname_buf,save->ncfile);
fprintf(stderr,"netcdf: varid=%d, time_ind=%d, lev_ind=%d, pack=%d\n",
varid,time_ind,level_ind,var_pack);
#endif
    save->nid++;
  
  return 0;
}

/******************************************************************************************/
static int check_nc_latlon( int ncid, int y_dim, int x_dim, int dim_latlon )
{
 /*
  * Check does existing lat and lot definition in open netcdf file
  * is consistent  with the new coming data?
  *
  * Sergey Varlamov
  */
  int i, nco_err;
  size_t nyy, nxx;
  double * test_ll, dd;
  const double test_d=1e-6;
  size_t start[2], count[2];
  int lat_var, lon_var;

#ifdef DEBUG_NC
fprintf(stderr,"netcdf: step in check_nc_latlon\n");
#endif

  nco_err=0;
  /* locate dimensions and corresponding variables with fixed names */

  if (dim_latlon == 1) i = nx > ny ? nx:ny;
  else i = nx*ny;

  test_ll = (double*) malloc(i*sizeof(double));
  if (!test_ll) fatal_error("netcdf:check_nc_latlon: %s","error doing malloc of test_ll");

  // expect lat and lon dimension variables
  netcdf_func( nc_inq_dimlen (ncid, x_dim, &nxx) );
  netcdf_func( nc_inq_dimlen (ncid, y_dim, &nyy) );
  netcdf_func( nc_inq_varid (ncid, "longitude", &lon_var) );
  netcdf_func( nc_inq_varid (ncid, "latitude", &lat_var) );

#ifdef DEBUG_NC
fprintf(stderr,"netcdf:check_nc_latlon: x,lon,nx,nxx,y,lat,ny,nyy=%d %d %d %lu %d %d %d %lu\n",
x_dim,lon_var,nx,(unsigned long)nxx,y_dim,lat_var,ny,(unsigned long)nyy);
#endif

  if (nyy != ny || nxx != nx){
    nco_err+=2;
  }
  else if (dim_latlon == 1)
  {
      start[0] = 0; count[0] = nx;
      netcdf_func( nc_get_vara_double(ncid, lon_var, start, count, test_ll) );
      if (nx > 1) dd = 0.01*fabs(lon[1]-lon[0]);
      else        dd = test_d*fabs(lon[0]);
      for (i=0; i<nx ; i++)
      {
        if(fabs(lon[i]-test_ll[i]) > dd )
        {
          fprintf(stderr,"different grid (longitude) in existing netcdf!\n");
          nco_err+=4;
          break;
        }
      }
#ifdef DEBUG_NC
fprintf(stderr,"netcdf:check_nc_latlon: longitude=%lf %lf\n",
test_ll[0],test_ll[nx-1]);
#endif
      start[0] = 0; count[0] = ny;
      netcdf_func( nc_get_vara_double(ncid, lat_var, start, count, test_ll) );
      /* use 1% of first step value as criteria */
      if (ny > 1) dd = 0.01*fabs(lat[nx]-lat[0]); /* 'nx' here is not error! */
      else        dd = test_d*fabs(lat[0]);      /* assumed tolerance */
      for (i=0; i<ny ; i++)
      {
        if(fabs(lat[i*nx]-test_ll[i]) > dd )
        {
          fprintf(stderr,"different grid (latitude) in existing netcdf!\n");
          nco_err+=8;
          break;
        }
      }
#ifdef DEBUG_NC
fprintf(stderr,"netcdf:check_nc_latlon: latitude=%lf %lf\n",
test_ll[0],test_ll[ny-1]);
#endif
  }
  else
  {
      start[0] = 0; count[0] = ny;
      start[1] = 0; count[1] = nx;
      netcdf_func( nc_get_vara_double(ncid, lat_var, start, count, test_ll) );
      /* use 1% of first step value as criteria */
      if (ny > 1) dd = 0.01*fabs(lat[nx]-lat[0]); /* 'nx' here is not error! */
      else        dd = test_d*fabs(lat[0]);      /* assumed tolerance */
      for (i=0; i<nx*ny ; i++)
      {
        if(fabs(lat[i]-test_ll[i]) > dd )
        {
          fprintf(stderr,"different grid (latitude) in existing netcdf!\n");
          nco_err+=16;
          break;
        }
      }
#ifdef DEBUG_NC
fprintf(stderr,"netcdf:check_nc_latlon: latitude=%lf %lf\n",
test_ll[0],test_ll[ny-1]);
#endif
      start[0] = 0; count[0] = ny;
      start[1] = 0; count[1] = nx;
      netcdf_func( nc_get_vara_double(ncid, lon_var, start, count, test_ll) );
      if (nx > 1) dd = 0.01*fabs(lon[1]-lon[0]);
      else        dd = test_d*fabs(lon[0]);
      for (i=0; i<nx*ny ; i++)
      {
        if(fabs(lon[i]-test_ll[i]) > dd )
        {
          fprintf(stderr,"different grid (longitude) in existing netcdf!\n");
          nco_err+=32;
          break;
        }
      }
#ifdef DEBUG_NC
fprintf(stderr,"netcdf:check_nc_latlon: longitude=%lf %lf\n",
test_ll[0],test_ll[nx-1]);
#endif
  }
  free(test_ll);

  if (nco_err)
  {
    fprintf(stderr,"*** Error appending data to existing netcdf file ***\n");
    fprintf(stderr,"*** Existing vise input dimensions [y,x]: [%lu, %lu]:[%d, %d] ***\n",
    (unsigned long)nyy,(unsigned long)nxx,ny,nx);
    fatal_error_i("netcdf:check_nc_latlon: dimension error: %d",nco_err);
  }
  return nco_err;
}

/******************************************************************************************/
static int update_nc_ref_time(int ncid, double verf_utime, double ref_utime,
                       int time_var, int time_step_type, double time_step)
{
/*
  Update time attributes if necessary and possible;
  nc_ref_time is reference time value,
  nc_ref_time_type is the reference time type:
     0 - undefined
     1 - analyses, all for the same reference date, could be succeded by forecasts
     2 - analyses for different reference date/time (time serie)
     3 - forecasts from the same reference date/time
     For the type 0 or 2 nc_ref_time keeps first input field reference date/time,
     could be misleading if not date0 field is coming first...
  Sergey Varlamov
*/
  int ok, update_rt, update_ts;
  double nc_ref_time, nc_time_step;
  int nc_ref_time_type;
  char * str;
  char ref_date[50];

#ifdef DEBUG_NC
fprintf(stderr,"netcdf: step in update_nc_ref_time\n");
#endif
  str = "";
  update_rt = 1;
  nc_ref_time=0;
  ok = nc_get_att_double(ncid, time_var, "reference_time", &nc_ref_time);
  if (ok != NC_NOERR) update_rt = 0; /* attribute do not exists, could be with update of file from older version */

  nc_ref_time_type=0;
  ok = nc_get_att_int(ncid, time_var, "reference_time_type", &nc_ref_time_type);
  if (ok != NC_NOERR || nc_ref_time_type <= 0) update_rt = 0;

  update_ts = 0;
  if (time_step_type == 0)
  { /* try to update only for "auto" case, skip if step is user-defined */
    nc_time_step=-1;  /* variable step, do not update */
    ok = nc_get_att_double(ncid, time_var, "time_step", &nc_time_step);
    if (ok == NC_NOERR && nc_time_step > -G2NC_TM_TOLERANCE) update_ts = 1;
  }
#ifdef DEBUG_NC
fprintf(stderr,
"netcdf:update_nc_ref_time: ref_time(f,c)=%.1lf %.1lf ref_time_type=%d time_step(f,c)=%.1lf %.1lf\n",
nc_ref_time, ref_utime,nc_ref_time_type,nc_time_step,time_step);
fprintf(stderr,"netcdf:update_nc_ref_time: update_rt=%d update_ts=%d\n",update_rt, update_ts);
#endif
  if ( !update_rt && !update_ts ) return 1;

  if (update_rt)
  {/* exists, value is defined - check it against new and,
      if it is not consistent - modify type */
    update_rt = 0;
    /* string for type 0 */
    str = "kind of product unclear, reference date is variable, min found reference date is given";
    switch (nc_ref_time_type)
    {
    case 1: /* 1 - analyses, all for the same date, could be succeded by forecasts */
      if ( fabs(nc_ref_time - ref_utime) > G2NC_TM_TOLERANCE &&
           fabs(ref_utime - verf_utime) < G2NC_TM_TOLERANCE )
      { /* change to analyses for different ref_time */
        nc_ref_time_type = 2;
        str = "analyses, reference date is variable, min found reference date is given";
        update_rt = 1;
      }
      else if (fabs(nc_ref_time - ref_utime) < G2NC_TM_TOLERANCE &&
               fabs(ref_utime - verf_utime) > G2NC_TM_TOLERANCE)
      { /* possibly it is first forecast after analyses */
        nc_ref_time_type = 3;
        str = "forecasts or accumulated (including analyses), reference date is fixed"; /*including analyses*/
        update_rt = 1;
      }
      else if (fabs(nc_ref_time - ref_utime) > G2NC_TM_TOLERANCE &&
               fabs(ref_utime - verf_utime) > G2NC_TM_TOLERANCE)
      { /* forecast for different reference time */
        nc_ref_time_type = 0;
        update_rt = 1;
      }
      break;
    case 2: /*2 - analyses, but for different reference date/time */
      if (fabs(ref_utime - verf_utime) > G2NC_TM_TOLERANCE)
      { /* mix, change type to undefined */
        nc_ref_time_type = 0;
        update_rt = 1;
      }
      break;
    case 3: /* 3 - forecasts from the same reference date/time */
      if (fabs(nc_ref_time - ref_utime) > G2NC_TM_TOLERANCE)
      { /* mix, change type to undefined */
        nc_ref_time_type = 0;
        update_rt = 1;
      }
      break;
    }
  }
  if (update_ts )
  {
    update_ts = 0;
    if (fabs(nc_time_step) < G2NC_TM_TOLERANCE && time_step > G2NC_TM_TOLERANCE)
    { /* attribute value is uninitialized (0), get valid value */
      nc_time_step = time_step;
      update_ts = 1;
    }
    else if (nc_time_step > G2NC_TM_TOLERANCE && time_step > G2NC_TM_TOLERANCE &&
             fabs(nc_time_step - time_step) > G2NC_TM_TOLERANCE)
    {
      nc_time_step = -1; /* variable time step, mark it as undefined */
      update_ts = 1;
    }
  }
#ifdef DEBUG_NC
fprintf(stderr,"netcdf:update_nc_ref_time: update_rt=%d update_ts=%d\n",update_rt, update_ts);
#endif
  if (update_rt || update_ts)
  {
    netcdf_func( nc_redef(ncid) );
    if (update_rt)
    {
      if (ref_utime < nc_ref_time) /* try to keep in file latest data reference time */
      {
        netcdf_func( nc_put_att_double(ncid, time_var, "reference_time", NC_DOUBLE, 1, &ref_utime) );
        get_unixdate(ref_utime, ref_date);
        netcdf_func( nc_put_att_text(ncid, time_var, "reference_date", strlen(ref_date), ref_date) );
      }
      netcdf_func( nc_put_att_int(ncid, time_var, "reference_time_type", NC_INT, 1, &nc_ref_time_type) );
      netcdf_func( nc_put_att_text(ncid, time_var, "reference_time_description", strlen(str), str) );
    }
    if (update_ts)
    {
      netcdf_func( nc_put_att_double(ncid, time_var, "time_step", NC_DOUBLE, 1, &nc_time_step) );
    }
    netcdf_func( nc_enddef(ncid) );
  }
  return 0;
}

/******************************************************************************************/
static int get_nc_time_ind(int ncid, double verf_utime, int time_var, int time_dim,
                    int * time_ind, double * last_verf_time,
                    double * time_step, int time_step_type, double date0 )
{
 /*
  * Compare provided verf_utime with this in open netcdf file and return
  * corresponding index for time dimension. If new value exceeds existing -
  * extend UNLIMITED time dimension writing new entry point(s).
  * Negative value is for errors.
  * Sergey Varlamov
  */
  int t_ind, i, j;
  double *wtime, ttime;
  size_t index[1], start[1], count[1], ntt;

#ifdef DEBUG_NC
fprintf(stderr,"netcdf: step in get_nc_time_ind\n");
#endif
  if (*time_ind < 0)
  { /* time dim is undefined yet */
    *last_verf_time = verf_utime-1;
    *time_ind = -1;
  }
  if(fabs(verf_utime - (*last_verf_time)) < G2NC_TM_TOLERANCE)
  {
    return (*time_ind); /* same as before, already defined */
  }
  /*
  Get the number of records written so far
  */
  netcdf_func( nc_inq_dimlen (ncid, time_dim, &ntt) );
  if (time_step_type)
  { /* user-defined, fixed, non-zero */
    i = (int)((verf_utime - date0)/(*time_step) + G2NC_TM_TOLERANCE);
#ifdef DEBUG_NC
fprintf(stderr,"netcdf:get_nc_time_ind: fixed time step, time_ind(r,i)=%.2lf %d ntt=%lu\n",
(verf_utime - date0)/(*time_step), i, ntt);
#endif
    if (fabs(date0+(*time_step)*i-verf_utime) > G2NC_TM_TOLERANCE)
    {
      return (-1); /* non-integer index */
    }
    /*
    Extend file if needed
    */
    if (i > ntt-1)
    { /* generate new time values */
      for (j=ntt-1; j <= i; j++)
      {
        index[0] = j;
        ttime = date0 + (*time_step)*j;
        netcdf_func( nc_put_var1_double(ncid, time_var, index, &ttime) );
      }
    }
    *time_ind = i;
    *last_verf_time = verf_utime;
    return i;
  }
  /* auto time stepping; could be irregular time-spaced data */
  index[0] = ntt-1;
  netcdf_func( nc_get_var1_double(ncid, time_var, index, &ttime) );

  if(verf_utime > ttime + G2NC_TM_TOLERANCE)
  {/* write new time entry point */
    t_ind = ntt;
    *time_ind = ntt;
    *time_step = verf_utime - ttime;
    *last_verf_time = verf_utime;
    index[0] = ntt;
    netcdf_func( nc_put_var1_double(ncid, time_var, index, last_verf_time) );
  }
  else
  { /* verf_utime <= ttime */
    t_ind = -1;
    wtime = (double *) malloc(ntt*sizeof(double));
    if (!wtime) fatal_error("netcdf:get_nc_time_ind: %s",
        "error doing malloc of wtime");

    start[0] = 0; count[0] = ntt;
    netcdf_func( nc_get_vara_double(ncid, time_var, start, count, wtime) );

    for (i = ntt-1; i >= 0; i--)
    {
      if (fabs(wtime[i]-verf_utime) < G2NC_TM_TOLERANCE)
      {
        t_ind = i;
        break;
      }
    }
    free(wtime);
    if (t_ind >= 0)
    {
      *time_ind = t_ind;
      *last_verf_time = verf_utime;
    }
  }
#ifdef DEBUG_NC
fprintf(stderr,"netcdf:get_nc_time_ind: variable time step, time_ind=%d ntt=%lu\n",
t_ind, ntt);
#endif

  return t_ind;
}

#endif
