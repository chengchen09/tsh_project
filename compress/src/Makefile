CC = gcc
MPICC = mpicc
PROGS = nc32bin lzma_compress extract lzma_decompress mpi_compress mpi_uncompress lzo_compress lzo_decompress bincat rotate_2d read_speed 
CFLAGS = -Wall -g 
CPPFLAGS = -I../lzmalib -I/home/bus/chen/local/include
LDFLAGS = -L../lzmalib -L/home/bus/chen/local/lib
LIBS = -llzma -llzo2
OBJS =
bindir = /home/bus/chen/local/bin

all: $(PROGS)
	cp $(PROGS) ../bin/

lzma_compress: lzma_compress.c
	$(CC) -o lzma_compress lzma_compress.c $(CFLAGS) $(CPPFLAGS) $(LDFLAGS) $(LIBS)

lzma_decompress: lzma_decompress.c
	$(CC) -o lzma_decompress lzma_decompress.c $(CFLAGS) $(CPPFLAGS) $(LDFLAGS) $(LIBS)

lzo_compress: lzo_compress.c
	$(CC) -o lzo_compress lzo_compress.c $(CFLAGS) $(CPPFLAGS) $(LDFLAGS) $(LIBS)

lzo_decompress: lzo_decompress.c
	$(CC) -o lzo_decompress lzo_decompress.c $(CFLAGS) $(CPPFLAGS) $(LDFLAGS) $(LIBS)

extract: extract.c
	$(CC) -o extract $(CFLAGS) extract.c

read_speed: read_speed.c
	$(CC) -o read_speed $(CFLAGS) read_speed.c

bincat: bincat.cpp
	g++ -o bincat $(CFLAGS) bincat.cpp

rotate_2d: rotate_2d.cpp
	g++ -o rotate_2d $(CFLAGS) rotate_2d.cpp

mpi_compress: mpi_compress.c
	$(MPICC) -o mpi_compress mpi_compress.c $(CFLAGS) $(CPPFLAGS) $(LDFLAGS) $(LIBS)

mpi_uncompress: mpi_uncompress.c
	$(MPICC) -o mpi_uncompress mpi_uncompress.c $(CFLAGS) $(CPPFLAGS) $(LDFLAGS) $(LIBS)

nc32bin: nc32bin.c
	$(MPICC) -o nc32bin nc32bin.c $(CFLAGS) -I/home/bus/chen/newnccopy/netcdf/include -L/home/bus/chen/newnccopy/netcdf/lib -lnetcdf -lhdf5_hl -lhdf5 -lcurl -lsz -lpnetcdf

install:
	cp $(PROGS) $(bindir)
clean:
	rm $(OBJS) $(PROGS)
