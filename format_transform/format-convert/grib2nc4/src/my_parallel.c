#include <stdio.h>
#include <mpi.h>

#include "my_parallel.h"

void set_hyperslab(size_t *count, size_t *offset, size_t *dimlens, int ndims)

{

	int size, rank;
	int i, cnt;

	MPI_Comm_size(MPI_COMM_WORLD, &size);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	/* the dimlen is not the multiples of size of processes */

	if((dimlens[0] % size) != 0){
		cnt = dimlens[0] / size;
		if(cnt == 0){
			count[0] = 1;
			offset[0] = (rank * count[0]) % dimlens[0];
		}
		else{
			cnt += 1;
			if(((size - 1) * cnt) < dimlens[0]){
				if((cnt * (rank + 1)) <= dimlens[0] )
					count[0] = cnt;
				else
					count[0] = dimlens[0] - cnt * rank;
				offset[0] = rank * cnt;
			}
			else{
				if(rank != (size - 1)){
					count[0] = cnt - 1;
				}
				else{ 
					count[0] = dimlens[0] - rank * (cnt - 1);
				}
				offset[0] = rank * (cnt - 1);
			}
		}
	
	}
	else {
		count[0] = dimlens[0] / size;
		offset[0] = rank * count[0];
	}
#ifdef DEBUG
	printf("process %d, offset[0] = %d, count[0] = %d\n",
				rank, offset[0], count[0]);
#endif
	for(i = 1; i < ndims; i++){
		count[i] = dimlens[i];
		offset[i] = 0;
#ifdef DEBUG
		printf("process %d, offset[%d] = %d, count[%d] = %d\n",
				rank, i, offset[i], i, count[i]);
#endif
	}

}
