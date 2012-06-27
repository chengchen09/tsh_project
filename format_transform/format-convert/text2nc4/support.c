/***********************************************************************
* Filename : support.c
* Create : Chen Cheng 2011-04-22
* Description: 
* Modified   : 
* Revision of last commit: $Rev$ 
* Author of last commit  : $Author$ $date$
* licence :
$licence$
* **********************************************************************/

#include "support.h"

void set_hyperslab(size_t *count, size_t *start, size_t *dimlens, int ndims, int size, int rank)
{
	int i, cnt;

	/* the dimlen is not the multiples of size of processes */
	if((dimlens[0] % size) != 0){
		cnt = dimlens[0] / size;
		if(cnt == 0){
			count[0] = 1;
			start[0] = (rank * count[0]) % dimlens[0];
		}
		else{
			cnt += 1;
			if(((size - 1) * cnt) < dimlens[0]){
				if((cnt * (rank + 1)) <= dimlens[0] )
					count[0] = cnt;
				else
					count[0] = dimlens[0] - cnt * rank;
				start[0] = rank * cnt;
			}
			else{
				if(rank != (size - 1)){
					count[0] = cnt - 1;
				}
				else{ 
					count[0] = dimlens[0] - rank * (cnt - 1);
				}
				start[0] = rank * (cnt - 1);
			}
		}
	
	}
	else {
		count[0] = dimlens[0] / size;
		start[0] = rank * count[0];
	}
	debug_printf("process %d, start[0] = %llu, count[0] = %llu\n",
				rank, start[0], count[0]);
	for(i = 1; i < ndims; i++){
		count[i] = dimlens[i];
		start[i] = 0;
		debug_printf("process %d, start[%d] = %llu, count[%d] = %llu\n",
				rank, i, start[i], i, count[i]);
	}

}

