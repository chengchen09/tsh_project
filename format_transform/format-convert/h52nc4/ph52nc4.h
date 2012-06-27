/*************************************
 * file:	ph52nc4.h
 * author:	Chen Cheng
 * data:	10/07/15
 * version:	
 *************************************/

#ifndef _H5TONC_H
#define _H5TONC_H

#include "hdf5.h"
#include "netcdf.h"

int h5_to_nc4(char *h5path, char *ncpath);

#endif
