CXX = gcc
RM = rm -f
CFLAGS = -c -O2 -Wall -D_7ZIP_ST
LIB = liblzma.a

OBJS = LzmaLib.o Alloc.o LzFind.o LzmaDec.o LzmaEnc.o 7zFile.o 7zStream.o

all: $(LIB)

$(LIB): LzmaLib.o Alloc.o LzFind.o LzmaDec.o LzmaEnc.o 7zFile.o 7zStream.o
	ar -r liblzma.a $(OBJS)
	ranlib liblzma.a

LzmaLib.o: LzmaLib.c
		$(CXX) $(CFLAGS) LzmaLib.c

Alloc.o: Alloc.c
		$(CXX) $(CFLAGS) Alloc.c

LzFind.o: LzFind.c
		$(CXX) $(CFLAGS) LzFind.c

LzmaDec.o: LzmaDec.c
		$(CXX) $(CFLAGS) LzmaDec.c

LzmaEnc.o: LzmaEnc.c
		$(CXX) $(CFLAGS) LzmaEnc.c

7zFile.o: 7zFile.c
		$(CXX) $(CFLAGS) 7zFile.c

7zStream.o: 7zStream.c
		$(CXX) $(CFLAGS) 7zStream.c

clean:
		-$(RM) $(LIB) $(OBJS)

