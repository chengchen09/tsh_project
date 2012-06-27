#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.65])
AC_INIT([ncgrep], [1.0])
AM_INIT_AUTOMAKE([-Wall -Werror])

AC_CONFIG_SRCDIR([src/grep.c])
AC_CONFIG_HEADERS([config.h])

dnl Check for arguments
AC_ARG_ENABLE(perl-regexp,
 [  --disable-perl-regexp           disable perl-regexp],
 [case "${enableval}" in
  yes) testpcre=yes ;;
  no)  testpcre=no ;;
  *)   AC_MSG_ERROR(bad value ${enableval} for --disable-perl-regexp) ;;
  esac],[testpcre=yes])

dnl Checks for programs.
#AC_CANONICAL_HOST
AC_PROG_AWK
AC_PROG_CC
AC_PROG_INSTALL
AC_PROG_RANLIB

dnl Checks for typedefs, structures, and compiler characteristics.
AC_SYS_LARGEFILE
AM_C_PROTOTYPES
AC_TYPE_SIZE_T
AC_CHECK_TYPE(ssize_t, int)
AC_C_CONST
#jm_AC_TYPE_UINTMAX_T

dnl Checks for header files.
AC_HEADER_STDC
AC_CHECK_HEADERS(string.h stdlib.h sys/param.h memory.h unistd.h libintl.h)
AC_CHECK_HEADERS(wctype.h wchar.h)
AC_HEADER_DIRENT
AC_HEADER_STAT

dnl Checks for functions.
AC_FUNC_ALLOCA
AC_FUNC_CLOSEDIR_VOID
AC_FUNC_MMAP

dnl getpagesize is checked for by AC_FUNC_MMAP.
AC_CHECK_FUNCS(btowc isascii iswctype mbrlen memmove setmode strerror wcrtomb wcscoll wctype)
# We can use mbrtowc only if we have mbstate_t.
AC_FUNC_MBRTOWC
# Yet we need a mbstate_t stub in every case.
#AC_MBSTATE_T
AC_REPLACE_FUNCS(memchr stpcpy strtoul atexit fnmatch)
#jm_AC_PREREQ_XSTRTOUMAX

dnl Replace this with jm_CHECK_DECLS once autoconf 2.15 is out.
#jm_CHECK_DECLARATIONS([#include <stdlib.h>], [strtoul strtoull])
test $jm_cv_func_decl_strtoul != yes
AC_DEFINE_UNQUOTED([HAVE_DECL_STRTOUL], $?,
  [Define if <stdlib.h> declares strtoul.])
test $jm_cv_func_decl_strtoull != yes
AC_DEFINE_UNQUOTED([HAVE_DECL_STRTOULL], $?,
  [Define if <stdlib.h> declares strtoull.])

dnl I18N feature
ALL_LINGUAS="af be bg ca cs da de el eo es et eu fi fr ga gl he hr hu id it ja ko nb nl no pl pt_BR pt ro ru rw sl sr sv tr uk vi zh_TW"
#AM_GNU_GETTEXT

dnl Many GNU/Linux people have different
dnl glibc versions with buggy regex.
#jm_INCLUDED_REGEX(lib/regex.c)

dnl Many people on non-GNU/Linux systems don't have getopt
AC_CHECK_FUNC(getopt_long,
  [
    AC_ARG_WITH(included-getopt,
      [  --with-included-getopt  Use the included getopt rather than glibc's],
      with_getopt=$withval,
      with_getopt=$no)
    if test "x$with_getopt" = xyes; then
      AC_LIBOBJ(getopt)
      AC_LIBOBJ(getopt1)
    fi
  ],
  [
    AC_LIBOBJ(getopt)
    AC_LIBOBJ(getopt1)
  ])

dnl Some installers want to be informed if we do not use our regex.
dnl For example, if the host platform uses dynamic linking and the installer
dnl knows that the grep may be invoked on other hosts with buggy libraries,
dnl then the installer should configure --with-included-regex.
if test "$jm_with_regex" = no; then
	AC_MSG_WARN(Included lib/regex.c not used)
fi

dnl These are the prerequisite macros for GNU's error.c file.
AC_FUNC_STRERROR_R
jm_PREREQ_ERROR

dnl Determine whether malloc accepts 0 as its argument.
dnl If it doesn't, arrange to use the replacement function.
jm_FUNC_MALLOC
jm_FUNC_REALLOC

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
	CPPFLAGS="$CPPFLAGS -I$DEP_DIR/include"
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
	AC_SUBST(HDF5_DIR, [$HDF5_DIR])
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

# Checks for header files.
AC_CHECK_HEADERS([OS.h fcntl.h libintl.h limits.h locale.h memory.h stddef.h stdlib.h string.h strings.h sys/param.h sys/time.h unistd.h wchar.h wctype.h])


AC_CONFIG_FILES([Makefile src/Makefile])
AC_OUTPUT
