/*
 File: $Id$
 Author: John Wu <John.Wu at acm.org>
      Lawrence Berkeley National Laboratory
 Copyright 2006-2011 the Regents of the University of California
*/
/**
   @file tcapi.c

   A simple test program for functions defined in capi.h.

   The basic command line options are
   @code
   datadir selection-conditions [<column type> <column type>...]
   @endcode

   Types recognized are: i (for integers), u (for unsigned integers), l
   (for long integers), f (for floats) and d for (doubles).  Unrecognized
   types are treated as integers.

    @ingroup FastBitExamples
*/
#include <capi.h>
#include <ctype.h>
#include <string.h>
#include <stdlib.h>

static void builtin(const char *nm, FILE* output) {
    int nerrors = 0;
    int i, mult;
    int32_t ivals[100];
    int16_t svals[100];
    float fvals[100];
    const char *dir = "tmp";
    int counts[] = {5, 24, 19, 10, 50};
    const char* conditions[] =
	{"a<5", "a+b>150", "a < 60 and c < 60", "c > 90", "c > a"};

    // prepare a sample data
    for (i = 0; i < 100; ++ i) {
	ivals[i] = i;
	svals[i] = (int16_t) i;
	fvals[i] = (float) (1e2 - i);
    }
    fastbit_add_values("a", "int", ivals, 100, 0);
    fastbit_add_values("b", "short", svals, 100, 0);
    fastbit_add_values("c", "float", fvals, 100, 0);
    fastbit_flush_buffer(dir);
    // test the queries
    mult = fastbit_rows_in_partition(dir);
    if (mult % 100 != 0) { /* no an exact multiple */
	fprintf(output, "Directory %s contains %d rows, but expected 100, "
		"remove the directory and try again\n", dir, mult);
	return;
    }

    mult /= 100;
    for (i = 0; i < 5; ++ i) {
	FastBitQueryHandle h = fastbit_build_query(0, dir, conditions[i]);
	int nhits = fastbit_get_result_rows(h);
	if (nhits != mult * counts[i]) {
	    ++ nerrors;
	    fprintf(output, "%s: query \"%s\" on %d built-in records found "
		    "%d hits, but %d were expected\n", nm, conditions[i],
		    (int)(mult*100), nhits, (int)(mult*counts[i]));
	}
	fastbit_destroy_query(h);
    }

} // builtin

int main(int argc, char **argv) {
    int ierr, nhits, vselect;
    const char *conffile;
    const char *logfile;
    FILE* output;
    FastBitQueryHandle qh;
    FastBitResultSetHandle rh;

    ierr = 0;
    vselect = 1;
    logfile = 0;
    conffile = 0;
#if defined(DEBUG) || defined(_DEBUG)
#if DEBUG + 0 > 10 || _DEBUG + 0 > 10
    ierr = INT_MAX;
#elif DEBUG + 0 > 0
    ierr += 7 * DEBUG;
#elif _DEBUG + 0 > 0
    ierr += 5 * _DEBUG;
#else
    ierr += 3;
#endif
#endif
    fastbit_init((const char*)conffile);
    fastbit_set_verbose_level(ierr);
    fastbit_set_logfile(logfile);
#if defined(TCAPI_USE_LOGFILE)
    output = fastbit_get_logfilepointer();
    printf("%s: output=0x%8.8x, stdout=0x%8.8x\n", *argv, output, stdout);
#else
    output = stdout;
#endif
    
    builtin(*argv, output);
    return 0;
} /* main */
