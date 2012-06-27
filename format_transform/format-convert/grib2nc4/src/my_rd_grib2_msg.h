#ifndef _MY_RD_GRIB2_MSG_H_
#define _MY_RD_GRIB2_MSG_H_

#include <mpi.h>

/*typedef int G2;

int g2mpiio_open(MPI_Comm     comm,
             const char  *g2path,
             int          ioflags,
             MPI_Info     info,
             G2       **nciopp);

int g2mpiio_get_vara();

int g2mpiio_close();*/

unsigned char *mpi_rd_grib2_msg(MPI_File *input, long int *pos, unsigned long int *len, int *num_submsgs);
int mpi_parse_1st_msg(unsigned char **sec);
int mpi_parse_next_msg(unsigned char **sec);
unsigned char *mpi_seek_grib2(MPI_File *file, MPI_Offset *pos, unsigned long int *len_grib, 
        unsigned char *buffer, unsigned int buf_len, long int *n_bytes);

#endif
