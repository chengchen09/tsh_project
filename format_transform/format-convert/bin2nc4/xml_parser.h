/***********************************************************************
* Filename : xml_parser.h
* Create : Chen Cheng 2011-04-19
* Description: 
* Modified   : 
* Revision of last commit: $Rev$ 
* Author of last commit  : $Author$ $date$
* licence :
$licence$
* **********************************************************************/

#ifndef _XML_PARSER_H
#define _XML_PAESER_H

#include <netcdf.h>

#define DIM_MAX_NUM 5
#define ATT_MAX_NUM 20
#define VAR_MAX_NUM 5

struct dim_xml_t {
	char *name;
	char *length;
};

struct att_xml_t {
	char *name;
	char *value;
};

struct var_xml_t {
	char *name;
	char *shape;
	char *type;
	int natts;
	struct att_xml_t att_list[ATT_MAX_NUM];
};

struct var_dat_t {
	long nelmts;
	nc_type type;
	int ndims;
	int stride;
	int varid;
	size_t *dim_lens;
	char **dim_names;
	short *sht_data;
	int *int_data;
	long long *i64_data;
	unsigned short *usht_data;
	unsigned int *uint_data;
	unsigned long long *ui64_data;
	float *flt_data;
	double *dbl_data;
	void *data;
};

int parser_xml_file(char *path);

/*struct dim_list_t{
	dim_xml_t *head;
	dim_xml_t *tail;
	dim_xml_t *next;
};

struct att_list_t{
	att_xml_t *head;
	att_xml_t *tail;
	att_xml_t *next;
};

struct var_list_t{
	var_xml_t *head;
	att_xml_t *tail;
	var_xml_t *next;
};*/
#endif
