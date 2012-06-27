
CFLAGS = -g -Wall
PROG = locality_indice count_fillvalue 

ALL: ${PROG}

locality_indice: locality_indice.cpp
	g++ ${CFLAGS} -o locality_indice locality_indice.cpp

count_fillvalue: count_fillvalue.cpp
	g++ ${CFLAGS} -o count_fillvalue count_fillvalue.cpp

copy:
	cp ${PROG} /home/bus/chen/local/bin/
clean:
	rm locality_indice
