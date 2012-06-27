BOOST_DIR = /usr
THRIFT_DIR = /home/bus/chen/local/include/thrift
LDFLAGS = -L/home/bus/chen/local/lib -Wl,-rpath=/home/bus/chen/local/lib
CFLAGS = -g  -Wall #-O2
PROG = arraydb_server arraydb_client simple_client
LIBS = -lthrift -lthriftnb -levent -lfastbit -llzma
CPPFLAGS = -I${THRIFT_DIR} -I${BOOST_DIR} -I/home/bus/chen/local/include

GEN_SRC = ArrayDB.cpp arraydb_types.cpp 

OBJS = Compressor.o arraydb_support.o ArrayDBHandler.o ArrayDB.o arraydb_types.o thread_pool.o

all: ${PROG}

arraydb_server: arraydb_server.cpp ${OBJS} 
	g++ $(CFLAGS) -o arraydb_server ${CPPFLAGS} arraydb_server.cpp ${OBJS} ${LDFLAGS} ${LIBS}

arraydb_client: arraydb_client.cpp ${OBJS}
	g++ $(CFLAGS) -o arraydb_client ${CPPFLAGS} arraydb_client.cpp ${OBJS} ${LDFLAGS} ${LIBS}

simple_client: simple_client.cpp ${OBJS}
	g++ $(CFLAGS) -o simple_client ${CPPFLAGS} simple_client.cpp ${OBJS} ${LDFLAGS} ${LIBS}

ArrayDBHandler.o: ArrayDBHandler.cpp global.h
	g++ $(CFLAGS) -c ArrayDBHandler.cpp ${CPPFLAGS}

arraydb_support.o: arraydb_support.cpp
	g++ $(CFLAGS) -c arraydb_support.cpp ${CPPFLAGS}

ArrayDB.o: ArrayDB.cpp
	g++ $(CFLAGS) -c ArrayDB.cpp ${CPPFLAGS}
	
arraydb_types.o: arraydb_types.cpp
	g++ $(CFLAGS) -c arraydb_types.cpp ${CPPFLAGS}
	
Compressor.o: Compressor.h Compressor.cpp
	g++ $(CFLAGS) -c Compressor.cpp ${CPPFLAGS}

thread_pool.o: thread_pool.h thread_pool.c
	g++ $(CFLAGS) -c thread_pool.c ${CPPFLAGS}

copy: 
	python stop_arraydb.py
	cp arraydb.conf ../scripts/launch_arraydb.py arraydb_server ../master
	cp arraydb.conf ../scripts/launch_arraydb.py arraydb_server ../worker-1
	cp arraydb.conf ../scripts/launch_arraydb.py arraydb_server ../worker-2

copy-client:
	cp simple_client experiments.py ../client

clean:
	$(RM) ${OBJS} ${PROG}
