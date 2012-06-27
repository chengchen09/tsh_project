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

#define LSBFIRST 0
#define MSBFIRST 1

#ifdef DEBUG
#define debug_printf(...)	printf(__VA_ARGS__)
#else
#define debug_printf(...)
#endif

#define swap16(s) ((((s) & 0xff) << 8) | (((s) >> 8) & 0xff))  
#define swap32(l) (((l) >> 24) | \
			(((l) & 0x00ff0000) >> 8)  | \
			(((l) & 0x0000ff00) << 8)  | \
			((l) << 24))  
#define swap64(ll) (((ll) >> 56) | \
			(((ll) & 0x00ff000000000000) >> 40) | \
			(((ll) & 0x0000ff0000000000) >> 24) | \
			(((ll) & 0x000000ff00000000) >> 8)  | \
			(((ll) & 0x00000000ff000000) << 8)  | \
			(((ll) & 0x0000000000ff0000) << 24) | \
			(((ll) & 0x000000000000ff00) << 40) | \
			(((ll) << 56)))

void set_hyperslab(size_t *count, size_t *start, size_t *dimlens, int ndims, int size, int rank);

#endif
