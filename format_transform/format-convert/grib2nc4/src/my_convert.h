/***********************************************************************
* Filename : my_convert.h
* Create : Chen Cheng 2011-04-24
* Description: 
* Modified   : 
* Revision of last commit: $Rev$ 
* Author of last commit  : $Author$ $date$
* licence :
$licence$
* **********************************************************************/
#ifndef MY_CONVERT_H
#define MY_CONVERT_H

#include "my_netcdf.h"

int convert_nc_info(unsigned char **sec, float *data, unsigned int ndata, char *inv_out, nc_info_t **local);

#endif
