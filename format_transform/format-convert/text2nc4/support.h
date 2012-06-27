/***********************************************************************
* Filename : support.h
* Create : Chen Cheng 2011-04-20
* Description: 
* Modified   : 
* Revision of last commit: $Rev$ 
* Author of last commit  : $Author$ $date$
* licence :
$licence$
* **********************************************************************/

#ifndef _SUPPORT_H
#define _SUPPORT_H
#include <stdio.h>

#ifdef DEBUG
#define debug_printf(...)	printf(__VA_ARGS__)
#else
#define debug_printf(...)
#endif

void set_hyperslab(size_t *count, size_t *start, size_t *dimlens, int ndims, int size, int rank);

#endif
