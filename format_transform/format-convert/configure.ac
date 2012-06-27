#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.65])
AC_INIT([format-convert], [1.0])
AM_INIT_AUTOMAKE([-Wall -Werror])

AC_CONFIG_SRCDIR([h52nc4/ph52nc4.c])

# Checks for programs.
AC_PROG_CC


dnl ---------------------------------------------------------------------
dnl all libdir

LIBS="$LIBS -lm"

AC_ARG_WITH([dependency],
			[AC_HELP_STRING([--with-dependency=DIR],
				[Specify path to all dependent library.])],
			[if test "$withval" != no; then
				if test "$withval" != yes; then
					DEP_DIR=$withval
				fi
			fi])

if test -n "$DEP_DIR"; then
	CPPFLAGS="$CPPFLAGS -I$DEP_DIR/include -I$DEP_DIR/include/libxml2"
	LDFLAGS="$LDFLAGS -L$DEP_DIR/lib -Wl,-rpath=$DEP_DIR/lib"
fi

dnl ---------------------------------------------------------------------
dnl szlib (for hdf5)

AC_ARG_WITH([szlib],
			[AC_HELP_STRING([--with-szlib=DIR],
				[Specify path to external szlib library.])],
			[if test "$withval" != no; then
				if test "$withval" != yes; then
					SZLIB_DIR=$withval
				fi
			fi])

if test -n "$SZLIB_DIR"; then
	CPPFLAGS="$CPPFLAGS -L$SZLIB_DIR/include"
	LDFLAGS="$LDFLAGS -L$SZLIB_DIR/lib"
	LIBS="$LIBS -lsz"	
	AC_CHECK_LIB(sz, SZ_BufftoBuffCompress,, AC_MSG_ERROR([invalid szlib]))
fi


dnl ---------------------------------------------------------------------
dnl zlib (for hdf5)
AC_ARG_WITH([zlib],
			[AC_HELP_STRING([--with-zlib=DIR],
							[Specify path to external zlib library.])],
			[if test "$withval" != no; then
				if test "$withval" != yes; then
					ZLIB_DIR=$withval
				fi
			fi])

if test -n "$ZLIB_DIR"; then
	CPPFLAGS="$CPPFLAGS -I$ZLIB_DIR/include"
	LDFLAGS="$LDFLAGS -L$ZLIB_DIR/lib"
	LIBS="$LIBS -lz"
fi

AC_CHECK_LIB([z], [compress2], [], [AC_MSG_ERROR([invalid zlib])])

dnl ---------------------------------------------------------------------
dnl libxml2
AC_ARG_WITH([xml2],
			[AC_HELP_STRING([--with-xml2=DIR],
							[Specify path to external xml2 library.])],
			[if test "$withval" != no; then
				if test "$withval" != yes; then
					XML2_DIR=$withval
				fi
			fi])

if test -n "$XML2_DIR"; then
	CPPFLAGS="$CPPFLAGS -I$XML2_DIR/include/libxml2"
	LDFLAGS="$LDFLAGS -L$XML2_DIR/lib"
	LIBS="$LIBS -lxml2"
fi

AC_CHECK_HEADERS([libxml/xmlreader.h],,AC_MSG_ERROR(cannot find xmlreader.h in xml2))
AC_CHECK_LIB([xml2], [xmlFreeTextReader], [], [AC_MSG_ERROR([invalid xml2 lib])])

dnl ---------------------------------------------------------------------
dnl hdf5

AC_ARG_WITH([hdf5],
			[AC_HELP_STRING([--with-hdf5=DIR],
				[Specify path to external hdf5 library.])],
			[if test "$withval" != no; then
				if test "$withval" != yes; then
					HDF5_DIR=$withval
				fi
			fi])

if test -n "$HDF5_DIR"; then
	CPPFLAGS="$CPPFLAGS -I$HDF5_DIR/include"
	LDFLAGS="$LDFLAGS -L$HDF5_DIR/lib"
	LIBS="$LIBS -lhdf5 -lhdf5_hl -lm"	
	AC_SUBST(HDF5_DIR, [$HDF5_DIR])
fi

AC_CHECK_HEADERS([hdf5.h],,AC_MSG_ERROR(cannot find hdf5.h in hdf5))
AC_CHECK_LIB([hdf5], [H5Fflush],,AC_MSG_ERROR([invalid hdf5 library in hdf5]))
AC_CHECK_LIB([hdf5_hl], [H5DSis_scale],, AC_MSG_ERROR([invalid hdf5 high-level library in hdf5]))

dnl ---------------------------------------------------------------------
dnl netcdf-4

AC_ARG_WITH([netcdf4],
			[AC_HELP_STRING([--with-netcdf4=DIR],
				[Specify path to external netcdf-4 library.])],
			[if test "$withval" != no; then
				if test "$withval" != yes; then
					NETCDF4_DIR=$withval
				fi
			fi])

if test -n "$NETCDF4_DIR"; then
	CPPFLAGS="$CPPFLAGS -I$NETCDF4_DIR/include"
	LDFLAGS="$LDFLAGS -L$NETCDF4_DIR/lib"
	LIBS="$LIBS -lnetcdf"
	AC_SUBST(NETCDF4_DIR, [$NETCDF4_DIR])
fi

AC_CHECK_HEADERS([netcdf.h],,AC_MSG_ERROR(cannot find netcdf.h in netcdf-4))
AC_CHECK_LIB(netcdf, nc_open,, AC_MSG_ERROR([invalid netcdf object in netcdf-4]))	


dnl ---------------------------------------------------------------------
dnl jasper lib (for pGrib2nc4)
AC_ARG_WITH([jasper],
			[AC_HELP_STRING([--with-jasper=DIR],
							[Specify path to external jasper library.])],
			[if test "$withval" != no; then
				if test "$withval" != yes; then
					JASPER_DIR=$withval
				fi
			fi])

if test -n "$JASPER_DIR"; then
	CPPFLAGS="$CPPFLAGS -I$JASPER_DIR/include"
	LDFLAGS="$LDFLAGS -L$JASPER_DIR/lib"
fi

AC_CHECK_HEADERS([jasper/jasper.h],,AC_MSG_ERROR(cannot find jasper.h in include directory))
AC_CHECK_LIB([jasper], [jas_init], [], [AC_MSG_ERROR([invalid jasper library])])


dnl ---------------------------------------------------------------------
dnl png lib (for pGrib2nc4)
AC_ARG_WITH([libpng],
			[AC_HELP_STRING([--with-libpng=DIR],
							[Specify path to external png library.])],
			[if test "$withval" != no; then
				if test "$withval" != yes; then
					LIBPNG_DIR=$withval
				fi
			fi])

if test -n "$LIBPNG_DIR"; then
	CPPFLAGS="$CPPFLAGS -I$LIBPNG_DIR/include"
	LDFLAGS="$LDFLAGS -L$LIBPNG_DIR/lib"
fi

AC_CHECK_HEADERS([png.h],,AC_MSG_ERROR(cannot find png.h in include directory))
AC_CHECK_LIB([png], [png_access_version_number], [], [AC_MSG_ERROR([invalid libpng])])


# Checks for header files.
AC_CHECK_HEADERS([stdlib.h string.h syslog.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_SIZE_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_CHECK_FUNCS([gethostname memset])

AC_CONFIG_HEADERS([config.h])
AC_CONFIG_FILES([Makefile h52nc4/Makefile bin2nc4/Makefile text2nc4/Makefile grib2nc4/Makefile grib2nc4/g2clib/Makefile grib2nc4/src/Makefile])
AC_OUTPUT
