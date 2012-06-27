/* wgrib2 main module:  w. ebisuzaki
 *
 * CHECK code is now duplicated
 *  if (decode) -- check before decoding
 *  if (!decode) -- check after processing one record so you can use wgrib2 to look at the field
 *
 * 1/2007 mods M. Schwarb: unsigned int ndata
 * 2/2008 WNE add -if support
 * 2/2008 WNE fixed bug in processing of submessages
 */

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

#include <assert.h>
#include <mpi.h>

/* #define DEBUG */
#define CHECK

/* global variables .. can be modified by funtions */
int mode=0;             /* -2=finalize, -1=initialize,  0 .. N is verbosity mode */
int header=1;           /* file header flag */
int save_translation = 0;

int for_mode = 0, for_start, for_end, for_step;
int for_n_mode = 0, for_n_start, for_n_end, for_n_step;

#ifdef USE_REGEX
int match = 0;
int match_flag = 0;
#endif

int only_submsg = 0;    /* if only_submsg > 0 .. only process submsg number (only_submsg) */


/* output file variables */

int file_append = 0;
int dump_msg = 0, dump_submsg = 0;
int ieee_little_endian = 0;

char *item_deliminator = ":";
char *nl = "\n\t";

/*
 * reads inventory and pulls out the address of the record
 * and submessage number
 */
int rd_inventory(int *rec_num, int *submsg, long int *pos) {

    long int tmp;
    int c, i;

    /* format: [digits].[submessage]:[pos]: or digits]:[pos]: */

    i = 0;

    c=getchar();
    while (c == ' ') c = getchar();
    if (c == EOF) return 1;
    if (!isdigit(c)) {
	fatal_error_i("bad inventory on line: %d",*rec_num);
    }

    /* record number */
    while (isdigit(c) ) {
	i = 10*i + c - '0';
        c=getchar();
    }

    *rec_num = i;

    if (c == '.') {
        i = 0;
	while (isdigit(c = getchar()) ) {
	    i = 10*i + c - '0';
	}
	*submsg = i;
    }
    else {
        *submsg = 1;
    }

    if (c != ':') fatal_error_i("bad inventory on line: %d",*rec_num);

    tmp = 0;
    while (isdigit(c = getchar()) ) {
        tmp = 10*tmp + c - '0';
    }

    /* if (c != ':') fatal_error_i("bad inventory on line: %d",*rec_num); */

    /* read the rest of the line */
    while (c != EOF && c != '\n') {
	c = getchar();
    }
    *pos = tmp;
    return 0;
}

void set_mode(int new_mode) {
	mode = new_mode;
}
