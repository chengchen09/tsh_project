#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stddef.h>
#include <math.h>
#include <float.h>

#include <assert.h>

#include "mpi.h"
#include "my_rd_grib2_msg.h"
#include "grb2.h"
#include "wgrib2.h"

#define G	71
#define R	82
#define I	       73
#define B	66

#define NTRY 100

unsigned char *mpi_seek_grib2(MPI_File *file, MPI_Offset *pos, unsigned long int *len_grib, 
        unsigned char *buffer, unsigned int buf_len, long int *n_bytes) {

    long int i, len, current_pos;
    int j, ret;
    MPI_Status status;

    for (j = 0; j < NTRY; j++) {

        //ret = MPI_File_seek(*file, *pos, MPI_SEEK_SET);
        if ((ret = MPI_File_seek(*file, *pos, MPI_SEEK_SET)) != MPI_SUCCESS) 
			break;
        // if ((*n_bytes = i = fread(buffer, sizeof(unsigned char), buf_len, file)) == 0) break;
        ret = MPI_File_read(*file, buffer, buf_len, MPI_BYTE, &status);
		if(ret != MPI_SUCCESS || status.count == 0)
            break;
        else
            *n_bytes = i = status.count;

        len = i - GB2_Sec0_size;
     
        for (i = 0; i < len; i++) {
            if (buffer[i] == G && buffer[i+1] == R && buffer[i+2] == I
                && buffer[i+3] == B) {
		if (buffer[i+7] == 2) {
                    *len_grib = uint8(buffer+i+8);
                    *pos += i;
                    return (buffer+i);
		}
		if (buffer[i+7] == 1) {
		    fprintf(stderr,"grib1 message ignored\n");
		}
            }
        }
	*pos = *pos + (buf_len - GB2_Sec0_size);
    }

    *len_grib = 0;
    return (unsigned char *) NULL;
}

/*
 * rd_grib2_msg.c *                              Wesley Ebisuzaki
 *
 * unsigned char rd_grib2_msg(FILE *input, long int pos, *int len)
 *
 * to read grib2 message
 *
 *    msg = rd_grib_msg(input, position, &len)
 *
 *    input is the file
 *    position is the byte location (should be set to zero for first record 
 *    msg = location of message
 *
 *    to get next record: *  position = position + len;
 *
 * rd_grib_msg allocates its own buffer which can be seen by the
 * parsing routines
 *
 * 1/2007 cleanup M. Schwarb
 * 6/2009 fix repeated bitmaps W. Ebisuzuaki
 */

#define BUFF_SIZE	(1024*64)
#define SEEK_LEN 2048

static unsigned char *buffer = NULL, *Msg = NULL, *Sec[9], *Sec6_bitmap;
static int buffer_size = 0;

unsigned char *mpi_rd_grib2_msg(MPI_File *input, long int *pos, unsigned long int *len, int *num_submsgs)
{

    unsigned long int len_grib, current_pos;
    long int i;
    unsigned long int tmp;
    long int n_bytes;
    unsigned char *p, *end_of_msg;
    
    MPI_Offset position;
    MPI_Status status;

    int rank, ret;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    assert(rank >= 0);
    
	ret = -1;
	memset(&status, 0, sizeof(status));

        /* setup grib buffer */
        if (buffer == NULL) {
            if ((buffer = (unsigned char *) malloc(BUFF_SIZE)) == NULL) {
    	    fatal_error("not enough memory: rd_grib2_msg","");
    	}
            buffer_size = BUFF_SIZE;
        }

        /* find Msg and length of message */

        position = *pos;
        Msg = mpi_seek_grib2(input, &position, &len_grib, buffer, SEEK_LEN, &n_bytes);
        if (Msg == NULL) {
            *len = 0;
    	return NULL;
        }

        /* read all whole grib record .. to save I/O time, add to end of buffer */

        if (len_grib + Msg - buffer > buffer_size) {
    	tmp = Msg - buffer;
            buffer_size = len_grib + Msg - buffer + 5000;
            buffer = (unsigned char *) realloc((void *) buffer, buffer_size);
            if (buffer == NULL) fatal_error("rd_grib2_msg: ran out of memory","");
    	Msg = buffer + tmp;
        }
        
    if(rank == 0){
        if (MPI_File_seek(*input, *pos+n_bytes, MPI_SEEK_SET) != MPI_SUCCESS) 
               fatal_error("rd_grib2_msg seek, outside the file, bad grib file","");

        i = len_grib + Msg - buffer - n_bytes; 	/* no. of bytes need to read */
        if (i > 0 && (ret = MPI_File_read(*input, buffer+n_bytes, i, MPI_BYTE,  &status)) != MPI_SUCCESS) 
            fatal_error("rd_grib2_msg, read outside of file, bad grib file","");
		if(i > 0 && status.count != i)
            fatal_error("rd_grib2_msg, read outside of file, bad grib file","");
		//if(ret == MPI_SUCCESS)
		//	if(status.count != i) 
          //  fatal_error("rd_grib2_msg, read outside of file, bad grib file","");
	}

    MPI_Bcast(Msg, buffer_size - (Msg - buffer), MPI_BYTE, 0, MPI_COMM_WORLD);

    Sec[8] = Msg + len_grib - 4;
    if (Sec[8][0] != 55 || Sec[8][1] != 55 || Sec[8][2] != 55 || Sec[8][3] != 55) {
        fatal_error("rd_grib2_msg, missing end section ('7777')","");
    }
    Sec[0] = Msg;

    /* scan message for number of submessages and perhaps for errors */
    p = Msg +  GB2_Sec0_size;
    end_of_msg = Msg + len_grib;

    i = 0;
    while (p < Sec[8]) {
	if (p[4] == 7) i++;
	p += uint4(p);
	if (p > end_of_msg) fatal_error("bad grib format","");
    }
    if (p != Sec[8]) {
	fprintf(stderr, "rd_grib2_msg: illegal format, end section expected\n");
	exit(8);
    }
    *num_submsgs = i;

/*    *len = len_grib + (position-*pos); */

    *pos = position;
    *len = len_grib;
    return Msg;
}



/*
 * with grib 1, a message = 1 field
 * with grib 2, a message can have more than one field
 *
 * this routine parses a grib2 message that has already been read into buffer
 *
 * parse_1st_msg .. returns 1st message starting at Msg
 */ 

int mpi_parse_1st_msg(unsigned char **sec) 
{

	unsigned char *p, *end_of_msg;
	int i;
 
	if (Msg == NULL) fatal_error("parse_1st_msg .. Msg == NULL","");

	Sec[0] = Msg;
	Sec[1] = Sec[2] = Sec[3] = Sec[4] = Sec[5] = Sec[6] = Sec[7] = 
	Sec6_bitmap = NULL;
	end_of_msg = Msg + GB2_MsgLen(Sec);

	p = Msg + 16;

	while (Sec[8] - p > 0) {
	    if (p[4] > 8) fatal_error_i("parse_1st_msg illegal section %d", (int) p[4]);
	    Sec[p[4]] = p;

	    /* Section 6: bitmap */
	    if (p[4] == 6) {
		if (p[5] == 0) {
		    Sec6_bitmap = p;
		}
		else if (p[5] >= 1 && p[5] <= 253) {
	            fatal_error("parse_1st_msg: predefined bitmaps are not handled","");
		}
		else if (p[5] == 254) {
	            fatal_error("parse_1st_msg: illegal grib msg, bitmap not defined code, table 6.0=254","");
		}
	    }

	    /* last section */
	    if (p[4] == 7) {
		for (i = 0; i < 9; i++) {
		    sec[i] = Sec[i];
		}
		return 0;
	    }
	    p += uint4(p);
	    if (p > end_of_msg) fatal_error("bad grib fill","");
	}
	fatal_error("parse_1st_msg illegally format grib","");
	return 1;
}

int mpi_parse_next_msg(unsigned char **sec) 
{

	unsigned char *p, *end_of_msg;
	int i;
 
	end_of_msg = Msg + GB2_MsgLen(sec);
	p = sec[7];
	if (p[4] != 7) {
            fatal_error("parse_next_msg: parsing error","");
	}
	p += uint4(p);
	if (p > end_of_msg) fatal_error("bad grib fill","");

	while (p < Sec[8]) {
	    Sec[p[4]] = p;

	    // code to handle code table 6.0
	    if (p[4] == 6) {
		if (p[5] == 0) {
		    Sec6_bitmap = p;
		}
		else if (p[5] >= 1 && p[5] <= 253) {
	            fatal_error("parse_next_msg: predefined bitmaps are not handled","");
		}
		else if (p[5] == 254) {
	            if (Sec6_bitmap == NULL) {
	                fatal_error("parse_1st_msg: illegal grib msg, bitmap not defined code, table 6.0=254","");
		    }
		    Sec[6] = Sec6_bitmap;
		}
	    }
	    if (p[4] == 7) {		// end of message .. save on sec[]
		for (i = 0; i < 9; i++) {
		    sec[i] = Sec[i];
		}
		return 0;
	    }
	    p += uint4(p);
	    if (p > end_of_msg) fatal_error("bad grib fill","");
	}
	return 1;
}

