#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <ctype.h>

#include "grb2.h"
#include "wgrib2.h"
#include "fnlist.h"
#include "grib2.h"
#include "my_netcdf.h"
#include "my_grib2nc.h"
#include "my_convert.h"
#include "my_defs.h"
#include "my_rd_grib2_msg.h"

#include <assert.h>
#include <mpi.h>

/* #define DEBUG */
//#define CHECK

/* global variables .. can be modified by funtions */


int flush_mode = 0;	/* flush of output 1 = yes */

int use_g2clib = USE_G2CLIB;	/* use g2clib code for decoding */
int fix_ncep_2_flag = 0;	
int fix_ncep_3_flag = 0;	
int fix_ncep_4_flag = 0;


int last_message = 0;	/* last message to process if set */

FILE *inv_file;


enum input_type input = all_mode;
enum output_order_type output_order = wesn, output_order_wanted = wesn;

char inv_out[INV_BUFFER]; 		/* inv functions write to this buffer */

int decode = 0;		/* decode grib file flag */



/* current grib location */
long int pos;
unsigned long int len;
int submsg; 
int msg_no;
int inv_no;

/* lat lon array .. used by Latlon.c to store location information */
double *lat = NULL, *lon = NULL;
int latlon = 0;
int new_GDS = 0;
int old_GDS_size = 0;
int GDS_max_size = 100;
unsigned char *old_gds;
int nx, ny, res, scan;
unsigned int npnts;
int use_scale = 0, dec_scale, bin_scale,  max_bits = 16, wanted_bits = 12;
enum output_grib_type grib_type = simple;


/* may disappear in future versions  grib2 decoder variables */
gribfield *grib_data;
int free_gribfield = 0;			// flag for allocated gribfield
int free_sec4 = 0;			// flag for allocated sec4


/* 
  * local varibles extracted from grib2nc
  */
 static int num_submsgs;
 static unsigned char *g2msg;
 static float *data;
 static unsigned int ndata;


int init_nc_info(int nc_version, nc_info_t *local, char *ncpath);
int parse_nc_info(MPI_File *in, unsigned char **sec);

  
int init_nc_info(int nc_version, nc_info_t *save, char *ncpath)
{

    assert(save);

    if (!save) 
	  fatal_error("netcdf: %s","error doing malloc of save");
    
	save->ncfile = (char*) malloc(STRLEN);
	//memset(save->ncfile, 0, STRLEN);
	if (!(save->ncfile)) 
	  fatal_error("netcdf: %s","error doing malloc of ncfile");
    strcpy(save->ncfile, ncpath);
	
	decode = latlon = 1;
    save->initialized = 0; /* not yet */
    save->ncid = -1; /* do not created or open */
    save->nid = 0;
    save->time_step = 0;
    save->time_step_type = 0;
    save->date0 = 0;
    save->date0_type = 0;

    save->nc_pack = 0;
    save->nc_pack_scale = 1;
    save->nc_pack_offset = 0;
    save->nc_valid_min = 0;
    save->nc_valid_max = 0;
    
    save->grid_template=-1;
    save->dim_latlon=0;
    save->lev_ind=-1;
    save->time_ind=-1;

    save->nc4 = nc_version;
    save->grads_compatible = 0;
    save->nc_nlev = 0;
    save->endianness=0;
  
    return 0;
}


int parse_nc_info(MPI_File *in, unsigned char **sec)
{
    unsigned char *msg;

    /*
     * safe definition
     */
    int i,j;
    unsigned int k;
    float missing_c_val_1, missing_c_val_2;
    double ref;
    g2float *g2_data;
    g2int *bitmap, has_bitmap;
    int err;

    nc_info_t *nc_info;
        
    /*
     * read the grib message into the memory
     * and pase the meta data section into sec variable
     */
    
    /* the first time of reading grib2 message */
	if (submsg == 0) {
	    if ((g2msg = mpi_rd_grib2_msg(in, &pos, &len, &num_submsgs)) == NULL) return BREAK;
            submsg = 1;
	}
   /* 
    * if a file include more than one grib2 messages, read it after the 
    *  previous message read 
	*/
	else if (submsg > num_submsgs) {
		pos += len;
        msg_no++;
	    if ((g2msg = mpi_rd_grib2_msg(in, &pos, &len, &num_submsgs)) == NULL) return BREAK;
            submsg = 1;
	}
    /* 
     * submsg mean a grib2 message has a repetive sections(2-7, 3-7, 4-7), if this
     * happens, need parse message many times
     */
    if (submsg == 1) {
		if (mpi_parse_1st_msg(sec) != 0) {
			fprintf(stderr,"illegal format: parsing 1st submessage\n");
		}
    }
    else {
		if (mpi_parse_next_msg(sec) != 0) {
            fprintf(stderr,"illegal format: parsing submessages\n");
        }
	}
	
	/* 
	 * see if new GDS 
	 */
	if ((i = GB2_Sec3_size(sec)) != old_GDS_size) {
	    new_GDS = 1;
	}
	else {
	    new_GDS = 0;
	    for (j = 0; j < i; j++) {
			if (old_gds[j] != sec[3][j]) 
			  new_GDS = 1;
	    }
	}
	if (new_GDS) {
	    if (i > GDS_max_size) {
			free(old_gds);
			GDS_max_size = i;
    		if ((old_gds = (unsigned char *) malloc(GDS_max_size) ) == NULL) {
				fatal_error("memory allocation problem old_gds","");
			}
	    }
	    for (j = 0; j < i; j++) {
			old_gds[j] = sec[3][j];
        }
	    old_GDS_size = i;
	    /* update grid information */
        get_nxny(sec, &nx, &ny, &npnts, &res, &scan);	 /* get nx, ny, and scan mode of grid */
	    output_order = (nx == -1 || ny == -1) ? raw : output_order_wanted;
        if (latlon) get_latlon(sec);			 /* get lat lon of grid points */
	}

	// any fixes to raw grib message before decode need to be placed here
	if (fix_ncep_2_flag) fix_ncep_2(sec);
	if (fix_ncep_3_flag) fix_ncep_3(sec);
	if (fix_ncep_4_flag) fix_ncep_4(sec);


	if (decode) {

#ifdef CHECK
		if (code_table_6_0(sec) == 0) {                         // has bitmap
            k = GB2_Sec3_npts(sec) -  GB2_Sec5_nval(sec);
            if (k != missing_points(sec[6]+6, GB2_Sec3_npts(sec))) 
				fatal_error_ii("inconsistent number of bitmap points sec3-sec5: %d sec6: %d",
								k, missing_points(sec[6]+6, GB2_Sec3_npts(sec)));
        }
        else if (code_table_6_0(sec) == 255) {                  // no bitmap
            if (GB2_Sec3_npts(sec) != GB2_Sec5_nval(sec))
                fatal_error_ii("inconsistent number of data points sec3: %d sec5: %d",
                    GB2_Sec3_npts(sec), GB2_Sec5_nval(sec));
        }
#endif

        /* allocate data */
        if (GB2_Sec3_npts(sec) != ndata) {
            if (ndata) free(data);
            ndata = GB2_Sec3_npts(sec);
            if (ndata) {
                data = (float *) malloc(ndata * sizeof(float));
                if (data == NULL) fatal_error("memory allocation failed data","");
            }
            else { data = NULL; }
        }

	    j = code_table_5_0(sec);		// type of compression

        /* 
         * set the data
         */
	  if ((j == 4 || j == 200) || ( (use_g2clib == 0) && (j == 0 || 
			j == 4 || j == 2 || j == 3 ||
			j == 40 || j == 41 || j == 40000))) {
				err = unpk_grib(sec, data);
                if (err != 0) {
                    fprintf(stderr,"Fatal decode packing type %d\n",j);
                    exit(8);
				}
	    }
        else {

	        err = g2_getfld(g2msg,submsg,1,1,&grib_data);
            if (err != 0) {
                fprintf(stderr,"Fatal decode err=%d msg %d.%d\n",err, msg_no, submsg);
                exit(8);
            }
			free_gribfield = 1;

	        has_bitmap = grib_data->ibmap;
	        g2_data = &(grib_data->fld[0]);
	        if (has_bitmap == 0 || has_bitmap == 254) {
				bitmap = grib_data->bmap;
		    	for (k = 0; k < ndata; k++) {
		    	     data[k] = (bitmap[k] == 0) ? UNDEFINED : g2_data[k];
		    	}
	        }
	        else {
				for (k = 0; k < ndata; k++) {
		    	    data[k] = *g2_data++;
		    	}
	        }

	        /* complex packing uses special values for undefined */
			i = sub_missing_values(sec, &missing_c_val_1, &missing_c_val_2);
			if (i == 1) {
			    for (k = 0; k < ndata; k++) {
			        if (data[k] == missing_c_val_1) data[k] = UNDEFINED;
			    }
			}
  	        else if (i == 2) {
				for (k = 0; k < ndata; k++) {
		    	    if (data[k] == missing_c_val_1) data[k] = UNDEFINED;
		    	    if (data[k] == missing_c_val_2) data[k] = UNDEFINED;
		    	}
	        }

        }

	    /* convert to standard output order we:sn */

	    if (output_order_wanted == wesn) 
			to_we_sn_scan(data);
	    else if (output_order_wanted == wens) 
			to_we_ns_scan(data);
	}
    else {
		if (ndata) 
			free(data);
    	ndata = 0;
    	data = NULL;
    }

    //data_p[0] = data;
    //ndata_p[0] = ndata;
	/* get scaling parameters */

	use_scale = scaling(sec, &ref, &dec_scale, &bin_scale, &i) == 0;


	if (num_submsgs > 1) {
	    fprintf(inv_file, "%d.%d%s%ld", msg_no, submsg, ":", pos);
	}
    else {
	    fprintf(inv_file, "%d%s%ld", msg_no, ":", pos);
	}
        
    return 0;
}

int grib2nc(char *g2path, char *ncpath){
    _grib2nc(g2path, ncpath, 1);
}

int _grib2nc(char *g2path, char *ncpath, int nc_version)
{
    //FILE *in;
    MPI_File in;
    unsigned char *msg, *sec[9];

    int i, j;
    double *ddata, ref;
    g2int *bitmap, has_bitmap;
    g2float *g2_data;

    int err;

    nc_info_t *nc_info;

    MPI_Init(NULL, NULL);
    
    nc_info = (nc_info_t *)malloc(sizeof(nc_info_t));
    assert(nc_info);

	inv_file = stdout;
    
    /*
     * initialize
     */
    init_nc_info(nc_version, nc_info, ncpath);

    /* open input file */
    /*if ((in = fopen(g2path,"rb")) == NULL) {
        fatal_error("could not open file: %s", g2path);
    }*/
    if ((err = MPI_File_open(MPI_COMM_WORLD, g2path, MPI_MODE_RDONLY, MPI_INFO_NULL, &in)) != MPI_SUCCESS) {
        fatal_error("could not open file: %s", g2path);
    }

    ndata = 0;
    data = NULL;
    msg_no = 1;
    inv_no = 0;
    len = pos = 0;
    submsg = 0;
    g2msg = NULL;

    if ((old_gds = (unsigned char *) malloc(GDS_max_size * sizeof(char)) ) == NULL) {
		fatal_error("memory allocation problem old_gds","");
    }

    /* 
     * submsg = 0 .. beginning of unread record
     * submsg = i .. start at ith submsg
     * num_submsgs = number of submessages in grib message
     */

    /* inventory loop */ 

    for (;last_message == 0;) {

		/* parse the message: metadata in sec and raw data in data */
       err = parse_nc_info(&in, sec);
	
	   if(err == BREAK)
		 break;

	   if(err == 8)
		 exit(err);    
        /*
         * transform
         */
	    //f_s(0, sec, NULL, 0, inv_out, NULL);
    	convert_nc_info(sec, data, ndata, inv_out, &nc_info);

		submsg++;

		if (free_sec4) { 
			free(sec[4]); free_sec4 = 0;
		}
		if (free_gribfield) { 
			g2_free(grib_data); 
			free_gribfield = 0;
		}

		fprintf(inv_file, "\n");
		if (flush_mode) fflush(inv_file);
		//if (dump_msg > 0) break;
    }

    /* finalize */
    finalize_nc_info(nc_info);

    MPI_File_close(&in);
    MPI_Finalize();
    
    return 0;
}

int main(int argc, char **argv) {
	int	ch, err;
    
    FILE *out = stdout;
    int nc_version;


    /* no arguments .. help screen */
    if (argc == 1) {
	fprintf(out, "Usage: pGrib2nc grib2-infile nc-outfile\n");
	exit(8);
    }

	nc_version = 1;

	if(argc == 2){
		char ncpath[STRLEN];
		int slen = strlen(argv[1]);
		memset(ncpath, 0, STRLEN * sizeof(char));
		strncpy(ncpath, argv[1], slen-4);
		strcat(ncpath, "nc");
		err = grib2nc(argv[1], ncpath);
	}
	else
	  err = grib2nc(argv[1], argv[2]);    
	
	/*ch = getopt(argc, argv, "34");
    if(ch == '3')
        nc_version = 0;
    else
	 nc_version = 1;*/

	
    //err = grib2nc(argv[1], argv[2], nc_version);
    
    return 0;
}

