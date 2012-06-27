#ifndef MATRIXROTATION_H
#define MATRIXROTATION_H

#include <iostream>
#include <assert.h>
#include <stdlib.h>

using namespace std;

namespace matrix
{
	class MatrixRotation
	{
		
		public:
			//Matrix(int nd, int *dlens, T *dt) = 0;
			virtual void matrix_rotation() = 0;
			virtual void print_original_matrix() = 0;
			virtual void print_rotated_matrix() = 0;
	};

	template <class T>
	class Matrix2D : public MatrixRotation
	{
		private:
			size_t ndims;
			size_t *dimlens;
			size_t nitems;
			size_t *rotated_dimlens;
			T *data;
			T *rotated_data;
		public:
			Matrix2D(size_t *dlens, T *dt) 
			{
				size_t i;

				ndims = 2;
				dimlens = dlens;
				data = dt;
				rotated_data = NULL;
				nitems = 1;
				for (i = 0; i < ndims; i++) {
					nitems *= dlens[i];
				}
			}
			
			~Matrix2D()
			{
				if (rotated_data != NULL)
					delete []rotated_data;
				if (rotated_dimlens != NULL)
					delete []rotated_dimlens;
				cout<<"~Matrix2D() called"<<endl;
			}
			
			void matrix_rotation()
			{
				if (!ndims || !nitems || !data) {
					cout<<"matrix get ndims == 0\n";
					exit(1);
				}
				
				size_t i, j;
				
				rotated_data = new T[nitems];
				assert(rotated_data != NULL);
				rotated_dimlens = new size_t[ndims];
				assert(rotated_dimlens != NULL);

				/* rotate dimension lens */
				for (i = 0, j = ndims -1; i < ndims; i++, j--) {
					rotated_dimlens[i] = dimlens[j];
				}

				/* rotate data */
				for (i = 0; i < dimlens[0]; i++)
					for (j = 0; j < dimlens[1]; j++)
						rotated_data[j * rotated_dimlens[1] + i] = data[i * dimlens[1] + j];
				
			}

			const T *get_rotated_data() {
				return (const T *)rotated_data;
			}
			
			void print_original_matrix()
			{
				size_t i, j;
				for (i = 0; i < dimlens[0]; i++) {
					for (j = 0; j < dimlens[1]; j++)
						cout<<data[i * dimlens[1] + j]<<"\t";
					cout<<endl;
				}
			}
			void print_rotated_matrix()
			{
				size_t i, j;
				for (i = 0; i < rotated_dimlens[0]; i++) {
					for (j = 0; j < rotated_dimlens[1]; j++)
						cout<<rotated_data[i * rotated_dimlens[1] + j]<<"\t";
					cout<<endl;
				}
			}

	};
}

#endif		
