
bin_PROGRAMS = ncgrep
ncgrep_SOURCES = grep.c search.c kwset.c dfa.c get_ncheader.c
noinst_HEADERS = grep.h dfa.h kwset.h getpagesize.h system.h mbsupport.h

LDADD = ../lib/libgreputils.a ../lib/libncdump.a
datadir = $(prefix)/@DATADIRNAME@
localedir = $(datadir)/locale

AM_CPPFLAGS = -I. -I.. -I../intl -I../lib -DLOCALEDIR=\"$(localedir)\"


