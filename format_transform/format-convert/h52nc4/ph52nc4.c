/******************************************
 * file:	ph52nc4.c
 * author:	Chen Cheng
 * date:	10/07/15
 * version: 
 ******************************************/

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <syslog.h>
#include <stdlib.h>

#ifdef DEBUG
#include <unistd.h>
#endif

#include "ph52nc4.h"
#include "hdf5.h"
#include "netcdf.h"
#include "visit.h"

/*
 * return code
 */
#define PARALLEL_TYPE	11
#define UNSUPPORT_TYPE	-11
#define VARIABLE_TYPE	-12
#define EXIT_CODE 1

/*
 * constants
 */
#define MAX_BUF_SIZE	1024*1024*1024

#define prefix_len		1024
#define NAME_LEN		256
#define DIM_NAME_LEN	256
#define VAR_NAME_LEN	256
#define DIM_SIZE		32	

#define TO_NC_ARRAY		-1

#define LOG_FILE		"./h5toNC.log"

/*
 * macro 
 */
#define MAX(a,b) (((a) > (b)) ? (a) : (b))
#define MIN(a,b) (((a) < (b)) ? (a) : (b))

#ifdef DEBUG
#define debug_printf(...)	printf(__VA_ARGS__)
#else
#define debug_printf(...)	NULL
#endif

#define my_printf(...) printf(__VA_ARGS__)

#ifdef SYSLOG
#define my_syslog(priority,...) do{	\
	if(global_rank == 0)	\
		syslog(priority,__VA_ARGS__);	\
	}while(0)
#else
#define my_syslog(priority,...) do{	\
	if(global_rank == 0)	\
		printf(__VA_ARGS__);	\
	}while(0)
#endif

#define NC_ASSERT(s) do{	\
	ret_val = (s);	\
	if(ret_val != 0){	\
		my_syslog(LOG_ERR, "%s:%d ERROR\n", __func__, __LINE__);	\
		my_printf("netcdf: ret_val = %d, %s\n", ret_val, nc_strerror(ret_val));	\
		assert(0);}	\
	}while(0)

#define H5_ASSERT(s) do{	\
	ret_val = (s);	\
	if(ret_val < 0){	\
		my_syslog(LOG_ERR, "%s:%d ERROR\n", __func__, __LINE__);	\
		my_printf("hdf5: ret_val = %d\n", ret_val);	\
		assert(0);}	\
	}while(0)

#define ASSERT(s)	do{	\
	if((s) == 0){	\
		my_syslog(LOG_ERR, "%s:%d ERROR\n", __func__, __LINE__);	\
		assert(0);}	\
	}while(0)	


/*
 * enum
 */
typedef enum objtype_t{
	GROUP,
	DATASET,
	NAMED_DATATYPE,
}objtype_t;

/* 
 * struct definition 
 */
typedef struct _trans_data_t{
	const char*	prefix;
	int		pa_ncid;
}trans_data_t;

typedef struct _trans_attr_t{
	objtype_t	type;		/* the type of owner of attribute */
	int			pa_ncid;	/* the parent id in nc file */
	int			varid;		/* dataset id */
	int			variable;	/* if 1, the data has variable length */
}trans_attr_t;

typedef struct _cmpd_field_t{
	char	*name;		/* field name */
	int		dtype_id;	/* data type id */
	size_t	offset;		/* field offset */
}cmpd_field_t;

static char *cords[] = {"_dim_0", "_dim_1", "_dim_2", "_dim_3", "_dim_4", 
						"_dim_5", "_dim_6", "_dim_7", "_dim_8", "_dim_9",
						"_dim_10", "_dim_11", "_dim_12", "_dim_13", "_dim_14", 
						"_dim_15", "_dim_16", "_dim_17", "_dim_18", "_dim_19",
						"_dim_20", "_dim_21", "_dim_22", "_dim_23", "_dim_24", 
						"_dim_25", "_dim_26", "_dim_27", "_dim_28", "_dim_29",
						"_dim_30", "_dim_31"};
static int	ret_val;
static int	global_rank;

static int		conv_group(int pa_h5id, int pa_ncid, const char *name, const char *fullname);
static int		conv_dataset(int pa_h5id, int pa_ncid, const char *name);
static int		conv_namedtype(int pa_h5id, int pa_ncid, const char *name);
static int		conv_attribute_cb(int pa_h5id, const char *name, const H5A_info_t *info, void *data);
static int		conv_all_cb(int pa_h5id, const char *name, const H5L_info_t *linfo, void *data);

static nc_type	get_nctype(int h5type);
static nc_type	get_nctype1(int h5type);
static int		get_inttype(int type);
static void	set_hyperslab(hsize_t *count, hsize_t *offset, hsize_t *dimlens, int ndims);

static int		define_nc_dtype(int pa_ncid, const char *name, int datatype_id, int nctype);
static int		define_nc_compound(int pa_ncid, const char *name, int datatype_id);
static int		define_nc_enum(int pa_ncid, const char *name, int datatype_id);
static int		define_nc_opaque(int pa_ncid, const char *name, int datatype_id);
static int		define_nc_vlen(int pa_ncid, const char *name, int datatype_id);

static void	write_nc_var(int dataset_id, int pa_ncid, const char *name, 
			int datatype_id, nc_type nctype);
static void	write_nc_char(int dataset_id, int pa_ncid, const char *name, 
			int datatype_id, nc_type nctype);
static void	write_nc_array(int dataset_id, int pa_ncid, const char *name, 
			int datatype_id, nc_type nctype);

static void	write_nc_attr(const char *name, int attr_id, int type, 
			int nctype, trans_attr_t *data_p);
static void	write_char_attr(const char *name, int attr_id, int type, 
			int nctype, trans_attr_t *data_p);
static void	write_array_attr(const char *name, int attr_id, int type, 
			int nctype, trans_attr_t *data_p);

static void	serial_trans(char *h5path, char *ncpath);
static int		serial_conv_group(int pa_h5id, int pa_ncid, const char *name);
static int		serial_conv_dataset(int pa_h5id, int pa_ncid, const char *name);
static int		serial_conv_all_cb(int pa_h5id, const char *name, const H5L_info_t *linfo, void *data);

static void	serial_write_compound(int dataset_id, int pa_ncid, const char *name, 
			int datatype_id, nc_type nctype);
static void	serial_write_string(int dataset_id, int pa_ncid, const char *name, 
			int datatype_id, nc_type nctype);
static void	serial_write_vlen(int dataset_id, int pa_ncid, const char *name, 
			int datatype_id, nc_type nctype);
static void	serial_write_array(int dataset_id, int pa_ncid, const char *name, 
			int datatype_id, nc_type nctype);

static int		check_datatype(int datatype_id, int nctype);
static int		check_compound(int dtype_id);
static int		check_vlen(int dtype_id);
static int		check_array(int dtype_id);

static void	format_name(char *str);
static void	delete_chr(char *str, char c);
static void	replace_chr(char *s, int old, int new);

int main(int argc, char *argv[])
{
    int ret = -1;

    /* check for input format */
	if(argc < 3){
		printf("Usage: ph52nc4 hdf5-infile nc4-outfile\n");
		return 1;
	}

	if(argc == 2){
		char ncpath[NAME_LEN];
		int len = strlen(argv[1]);
		memset(ncpath, 0, NAME_LEN * sizeof(char));
		strncpy(ncpath, argv[1], len-2);
		strcat(ncpath, "nc");
		ret = h5_to_nc4(argv[1], ncpath);
	}
	else
	  ret = h5_to_nc4(argv[1], argv[2]);

    return 0;
}

int h5_to_nc4(char *h5path, char *ncpath)
{
    int h5file, ncfile;	/* file id */
	int	rank, size;
	int	acc_tpl;	/* file acees property list */

	MPI_Comm	comm = MPI_COMM_WORLD;
	MPI_Info	mpi_info = MPI_INFO_NULL;

	MPI_Init(NULL, NULL);
	
	MPI_Comm_rank(MPI_COMM_WORLD, &global_rank);
	MPI_Comm_size(MPI_COMM_WORLD, &size);
#ifdef DEBUG
	char hostname[128];
	if(gethostname(hostname, 128) < 0){
	    printf("gethostname failed!\n");
	    return 1;
	}
	printf("hostname: %s\n", hostname);
#endif

	if(global_rank == 0){
		/* connect syslog */
		openlog("ph5toNC", LOG_CONS | LOG_PID | LOG_PERROR, LOG_LOCAL2);
	}

	/* setup file access template */
	H5_ASSERT(acc_tpl = H5Pcreate(H5P_FILE_ACCESS));	
	H5_ASSERT(H5Pset_fapl_mpio(acc_tpl, comm, mpi_info));

	/* open the h5 file*/
    H5_ASSERT((h5file = H5Fopen(h5path, H5F_ACC_RDONLY, acc_tpl)));	

	H5_ASSERT(H5Pclose(acc_tpl));	/* release file access template */

	/* create a nc file */
	NC_ASSERT(nc_create_par(ncpath, NC_NETCDF4 | NC_MPIIO, comm, mpi_info, &ncfile));

	/* initial id_map_class */
    init_visited();

	/* initial unvst_addrs */
	init_unvst_addrs();

	my_syslog(LOG_NOTICE, "ph5toNC: start group /\n");
	conv_group(h5file, ncfile, "/", "");	
  	my_syslog(LOG_NOTICE, "ph5toNC: end group /\n");

	/* close file */
    H5_ASSERT(H5Fclose(h5file));
    NC_ASSERT(nc_close(ncfile));

	/* close syslog */
	if(global_rank == 0)
		closelog();

	MPI_Finalize();
	
	if(check_revst() && global_rank == 0){
		if(global_rank == 0)
			openlog("ph5toNC", LOG_CONS | LOG_PID | LOG_PERROR, LOG_LOCAL2);
		
		my_syslog(LOG_NOTICE, "ph5toNC: Retransform the %s in serial mode\n", h5path);
		serial_trans(h5path, ncpath);
		
		if(global_rank == 0)
			closelog();
	}

	return 0;
}

int conv_group(int pa_h5id, int pa_ncid, const char *name, const char *fullname)
{
	int				gid, new_ncid;
	trans_data_t	tdata;
	trans_attr_t	tattr;
	H5O_info_t		objinfo;
	char			newname[VAR_NAME_LEN];

	tdata.pa_ncid = pa_ncid;
	tdata.prefix = fullname;

	/* open a group */
	H5_ASSERT(gid = H5Gopen2(pa_h5id, name, H5P_DEFAULT));
	
	/* get the group infomation */
    H5_ASSERT(H5Oget_info(gid, &objinfo));

	if(check_visited(objinfo.addr) == 0){
		set_visited(objinfo.addr);
		/* create a group in nc */
		if(strcmp(name, "/")){

			/* remove the illegal character */
			memset(newname, 0, VAR_NAME_LEN);
			strcpy(newname, name);
			format_name(newname);	

			NC_ASSERT(nc_def_grp(pa_ncid, newname, &new_ncid));
			tdata.pa_ncid = new_ncid;
		}

		tattr.pa_ncid = tdata.pa_ncid;
		tattr.type = GROUP;
		tattr.varid = -1;
		tattr.variable = 0;
		H5Aiterate(gid, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, conv_attribute_cb, &tattr);
		
		H5Literate(gid, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, conv_all_cb, &tdata);
	}

	H5_ASSERT(H5Gclose(gid));
	return 0;
}

int conv_dataset(int pa_h5id, int pa_ncid, const char *name)
{
	int			dataset_id, datatype_id;
	int			ret;
	H5O_info_t	objinfo;
	nc_type		nctype;
	char		newname[VAR_NAME_LEN];

	/* open dataset */
	H5_ASSERT(dataset_id = H5Dopen1(pa_h5id, name));
	
	/* get information of dataset */
	H5_ASSERT(H5Oget_info(dataset_id, &objinfo));

	/* 
	 * read the data from the h5 file, then put these data into a nc variable 
	 */
	if(check_visited(objinfo.addr) == 0){	
	
		set_visited(objinfo.addr);
		H5_ASSERT(datatype_id = H5Dget_type(dataset_id));	
		
		nctype = get_nctype(datatype_id);			

		/*
		 *	check variable type and unsupported type
		 */
		ret = check_datatype(datatype_id, nctype);

		if(ret == VARIABLE_TYPE){
			set_revst();
			
			/* record the address of object which need be revisited in serial mode */
			rcd_unvst_addr(objinfo.addr);

			my_syslog(LOG_NOTICE, "ph5toNC: meeting a variable length datatype\n");

			H5_ASSERT(H5Dclose(dataset_id));			
			return 0;
		}
		else if(ret == UNSUPPORT_TYPE){

			my_syslog(LOG_NOTICE, "ph5toNC: meeting a unsupport datatype in compound\n");

			H5_ASSERT(H5Dclose(dataset_id));
			return 0;
		}

		/*
		 * write variable
		 */
		
		/* remove the illegal character */
		memset(newname, 0, VAR_NAME_LEN);
		strcpy(newname, name);
		format_name(newname);	

		nctype = define_nc_dtype(pa_ncid, newname, datatype_id, nctype);
		
		write_nc_var(dataset_id, pa_ncid, newname, datatype_id, nctype);
				
		H5_ASSERT(H5Tclose(datatype_id));
	}/* end if(check_visited()) */

	H5_ASSERT(H5Dclose(dataset_id));
	return 0;
}

int conv_namedtype(int pa_h5id, int pa_ncid, const char *name)
{
	//int				dataset_id, datatype_id, nctype;
	int				dataset_id;
	H5O_info_t		objinfo;
	//trans_attr_t	tattr;
	
	H5_ASSERT(dataset_id = H5Topen2(pa_h5id, name, H5P_DEFAULT));

	H5_ASSERT(H5Oget_info(dataset_id, &objinfo));
	
	if(check_visited(objinfo.addr) == 0){
		set_visited(objinfo.addr);
		
		/*H5_ASSERT(datatype_id = H5Dget_type(dataset_id));	
		
		nctype = get_nctype(datatype_id);			

		tattr.pa_ncid = pa_ncid;
		tattr.type = NAMED_DATATYPE;
		tattr.varid = -1;
		H5Aiterate(dataset_id, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, conv_attribute_cb, &tattr);*/
		debug_printf("named type: %s\n", name);
	}

	H5_ASSERT(H5Tclose(dataset_id));
	return 0;
}

int	conv_attribute_cb(int pa_h5id, const char *name, const H5A_info_t *info, void *data)
{

	int				attr_id, datatype_id, nctype;
	int				ret;
	trans_attr_t	*data_p = (trans_attr_t *)data;
	char			newname[VAR_NAME_LEN];

	/*
	 * read attritute from h5
	 */
	H5_ASSERT(attr_id = H5Aopen(pa_h5id, name, H5P_DEFAULT));

	H5_ASSERT(datatype_id = H5Aget_type(attr_id));

	/*
	 * write attribute
	 */
	nctype = get_nctype(datatype_id);

	ret = check_datatype(datatype_id, nctype);
	if(ret == UNSUPPORT_TYPE){

		my_syslog(LOG_WARNING, "ph5toNC: WARING--meeting a unspport type in vlen\n");

		H5_ASSERT(H5Tclose(datatype_id));
		H5_ASSERT(H5Aclose(attr_id));
		return 0;
	}

	if(ret == VARIABLE_TYPE){
		data_p->variable = 1;
	}
	/* remove the illegal character */
	memset(newname, 0, VAR_NAME_LEN);
	strcpy(newname, name);
	format_name(newname);
	
	nctype = define_nc_dtype(data_p->pa_ncid, newname, datatype_id, nctype);
	
	write_nc_attr(newname, attr_id, datatype_id, nctype, data_p);
	
	H5_ASSERT(H5Tclose(datatype_id));
	H5_ASSERT(H5Aclose(attr_id));

	return 0;
}

int conv_all_cb(hid_t id, const char * name, const H5L_info_t *linfo, void *data)
{
    H5O_info_t	objinfo;
	trans_data_t *tdata_p = (trans_data_t *) data;

	const char*		prefix = tdata_p->prefix;
	char*		fullname;

    fullname = (char *)malloc(strlen(prefix) + strlen(name) + 2);
	sprintf(fullname, "%s/%s", prefix, name);


	H5_ASSERT(H5Oget_info(id, &objinfo));
    
	/* get object info */
	if(linfo->type == H5L_TYPE_HARD){   
        H5_ASSERT(H5Oget_info_by_name(id, name, &objinfo, H5P_DEFAULT));

        switch(objinfo.type){
        case H5O_TYPE_GROUP:	

			my_syslog(LOG_NOTICE, "ph5toNC: start group %s\n", fullname);

            conv_group(id, tdata_p->pa_ncid, name, fullname);

			my_syslog(LOG_NOTICE, "ph5toNC: end group %s\n", fullname);

			break;

        case H5O_TYPE_DATASET:

			my_syslog(LOG_NOTICE, "ph5toNC: start dataset %s\n", fullname);

			conv_dataset(id, tdata_p->pa_ncid, name);

			my_syslog(LOG_NOTICE, "ph5toNC: end dataset %s\n", fullname);

			break;

		case H5O_TYPE_NAMED_DATATYPE:

			my_syslog(LOG_NOTICE, "ph5toNC: start dataset %s\n", fullname);

			conv_namedtype(id, tdata_p->pa_ncid, name);

			my_syslog(LOG_NOTICE, "ph5toNC end dataset %s\n", fullname);

			break;

		case H5O_TYPE_UNKNOWN:

			my_syslog(LOG_WARNING, "ph5toNC: WARNING--meeting a unknown obj %s\n", fullname);
			break;

		case H5O_TYPE_NTYPES:

			my_syslog(LOG_WARNING, "ph5toNC: WARNING--meeting a NTYPES  %s\n", fullname);
			break;

        default:
			ASSERT(0);
			break;
        }
    }

	free(fullname);

	return 0;
}


nc_type	get_nctype(int h5type)
{
	nc_type		nctype = NC_NAT;
	int			type;
	int			size;

	if(H5Tget_class(h5type) ==  H5T_BITFIELD)
		return nctype;
	
	H5_ASSERT(type = H5Tget_native_type(h5type, H5T_DIR_DEFAULT));
	
	/* 
	 * float 
	 */
	if(H5Tequal(type, H5T_NATIVE_FLOAT))
	  nctype = NC_FLOAT;
	else if(H5Tequal(type, H5T_NATIVE_DOUBLE))
	  nctype = NC_DOUBLE;
	else if(H5Tequal(type, H5T_NATIVE_LDOUBLE))
	  nctype = NC_DOUBLE;

	/* 
	 * char
	 */
	else if(H5Tequal(type, H5T_NATIVE_SCHAR))
	  nctype = NC_BYTE;
	else if(H5Tequal(type, H5T_NATIVE_UCHAR))
	  nctype = NC_UBYTE;
	
	/* 
	 * string: fixed len and variable len
	 */
	else if(H5T_STRING == H5Tget_class(type)){
		if(H5Tis_variable_str(type))
		  nctype = NC_STRING;
		else
		  nctype = NC_CHAR;
	}

	/* 
	 * integer 
	 */
	else if(H5Tequal(type, H5T_NATIVE_INT))
	  nctype = NC_INT;
	else if(H5Tequal(type, H5T_NATIVE_UINT))
	  nctype = NC_UINT;
	else if(H5Tequal(type, H5T_NATIVE_SHORT))
	  nctype = NC_SHORT;
	else if(H5Tequal(type, H5T_NATIVE_USHORT))
	  nctype = NC_USHORT;
	else if(H5Tequal(type, H5T_NATIVE_LONG)){
	    if(sizeof(long) == sizeof(int))
		nctype = NC_INT;
	    else
		nctype = NC_INT64;
	}
	else if(H5Tequal(type, H5T_NATIVE_ULONG)){
	    if(sizeof(unsigned long) == sizeof(unsigned int))
		nctype = NC_UINT;
	    else
		nctype = NC_UINT64;
	}
	else if(H5Tequal(type, H5T_NATIVE_LLONG))
	  nctype = NC_INT64;
	else if(H5Tequal(type, H5T_NATIVE_ULLONG))
	  nctype = NC_UINT64;

	/*
	 * hsize_t 
	 */
	else if(H5Tequal(type, H5T_NATIVE_HSSIZE)){
		size = sizeof(hssize_t);
		if(size == sizeof(int))
		  nctype = NC_INT;
		/*else if(size == sizeof(long)){ 
		    nctype = NC_INT;
		}*/
		else if(size == sizeof(long long))
		  nctype = NC_INT64;
	}
	else if(H5Tequal(type, H5T_NATIVE_HSIZE)){
		size = sizeof(hsize_t);
		if(size == sizeof(unsigned int))
		  nctype = NC_UINT;
		/*else if(size == sizeof(long))
		  nctype = NC_UINT;*/
		else if(size == sizeof(long long))
		  nctype = NC_UINT64;
	}

	/* 
	 * opaque
	 */
	else if(H5Tget_class(type) ==  H5T_OPAQUE)
	  nctype = NC_OPAQUE;

	/* 
	 * compound 
	 */
	else if(H5Tget_class(type) == H5T_COMPOUND)
	  nctype = NC_COMPOUND;

	/* 
	 * enum
	 */
	else if(H5Tget_class(type) == H5T_ENUM)
	  nctype = NC_ENUM;

	/*
	 * variable length
	 */
	else if(H5Tget_class(type) == H5T_VLEN)
	  nctype = NC_VLEN;

	/*
	 * H5T_ARRAY
	 */
	else if(H5Tget_class(type) == H5T_ARRAY)
	  nctype = TO_NC_ARRAY;
#if 0
	/*
	 * not support type: H5T_REFERENCE, H5T_TIME, H5T_BITFIELD
	 */
	else if(H5Tget_class(type) == H5T_REFERENCE)
	  nctype = NC_UINT;
#endif

	H5_ASSERT(H5Tclose(type));
	return nctype;
}

nc_type get_nctype1(int h5type)
{
	if(H5Tget_class(h5type) == H5T_ARRAY)
	  return TO_NC_ARRAY;

	return get_nctype(h5type);
}

int get_inttype(int type)
{
	int filetype;
	if (H5Tequal(type, H5T_STD_I8BE)) {
		filetype = H5T_STD_I8BE;
    } 
	else if (H5Tequal(type, H5T_STD_I8LE)) {
		filetype = H5T_STD_I8LE;
	}
	else if (H5Tequal(type, H5T_STD_I16BE)) {
		 filetype = H5T_STD_I16BE;
    } 
	else if (H5Tequal(type, H5T_STD_I16LE)) {	
		filetype = H5T_STD_I16LE;
	} 
	else if (H5Tequal(type, H5T_STD_I32BE)) {
		filetype = H5T_STD_I32BE;
	} 
	else if (H5Tequal(type, H5T_STD_I32LE)) {
		filetype = H5T_STD_I32LE;	
	} 
	else if (H5Tequal(type, H5T_STD_I64BE)) {
		filetype = H5T_STD_I64BE;
	}
	else if (H5Tequal(type, H5T_STD_I64LE)) {
		filetype = H5T_STD_I64LE;
	} 
	else if (H5Tequal(type, H5T_STD_U8BE)) {
		filetype = H5T_STD_U8BE;
	} 
	else if (H5Tequal(type, H5T_STD_U8LE)) {
		filetype = H5T_STD_U8LE;
	} 
	else if (H5Tequal(type, H5T_STD_U16BE)) {
		filetype = H5T_STD_U16BE;
	} 
	else if (H5Tequal(type, H5T_STD_U16LE)) {
		filetype = H5T_STD_U16LE;
	} 
	else if (H5Tequal(type, H5T_STD_U32BE)) {
		filetype = H5T_STD_U32BE;
	} 
	else if (H5Tequal(type, H5T_STD_U32LE)) {
		filetype = H5T_STD_U32LE;
	} 
	else if (H5Tequal(type, H5T_STD_U64BE)) {
		filetype = H5T_STD_U64BE;
	} 
	else if (H5Tequal(type, H5T_STD_U64LE)) {
		filetype = H5T_STD_U64LE;	
	} 
	else if (H5Tequal(type, H5T_NATIVE_SCHAR)) {										
		filetype = H5T_NATIVE_SCHAR;											
	} 
	else if (H5Tequal(type, H5T_NATIVE_UCHAR)) {
		filetype = H5T_NATIVE_UCHAR;
	} 
	else if (H5Tequal(type, H5T_NATIVE_SHORT)) {
		filetype = H5T_NATIVE_SHORT;
	} 
	else if (H5Tequal(type, H5T_NATIVE_USHORT)) {
		filetype = H5T_NATIVE_USHORT;
	} 
	else if (H5Tequal(type, H5T_NATIVE_INT)) {
		filetype = H5T_NATIVE_INT;
	} 
	else if (H5Tequal(type, H5T_NATIVE_UINT)) {
		filetype = H5T_NATIVE_UINT;
	} 
	else if (H5Tequal(type, H5T_NATIVE_LONG)) {
		filetype = H5T_NATIVE_LONG;
	} 
	else if (H5Tequal(type, H5T_NATIVE_ULONG)) 
	{
		filetype = H5T_NATIVE_ULONG;
	} 
	else if (H5Tequal(type, H5T_NATIVE_LLONG)) {	
		filetype = H5T_NATIVE_LLONG;	
	} 
	else if (H5Tequal(type, H5T_NATIVE_ULLONG)) {
		filetype = H5T_NATIVE_ULLONG;	
	} 
	else {											
		filetype = -1;
	}
	
	return filetype;
}

void set_hyperslab(hsize_t *count, hsize_t *offset, hsize_t *dimlens, int ndims)
{
	int size, rank;
	int i, cnt;

	MPI_Comm_size(MPI_COMM_WORLD, &size);
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);

	/* the dimlen is not the multiples of size of processes */
	if((dimlens[0] % size) != 0){
		cnt = dimlens[0] / size;
		if(cnt == 0){
			count[0] = 1;
			offset[0] = (rank * count[0]) % dimlens[0];
		}
		else{
			cnt += 1;
			if(((size - 1) * cnt) < dimlens[0]){
				if((cnt * (rank + 1)) <= dimlens[0] )
					count[0] = cnt;
				else
					count[0] = dimlens[0] - cnt * rank;
				offset[0] = rank * cnt;
			}
			else{
				if(rank != (size - 1)){
					count[0] = cnt - 1;
				}
				else{ 
					count[0] = dimlens[0] - rank * (cnt - 1);
				}
				offset[0] = rank * (cnt - 1);
			}
		}
	
	}
	else {
		count[0] = dimlens[0] / size;
		offset[0] = rank * count[0];
	}
	debug_printf("process %d, offset[0] = %llu, count[0] = %llu\n",
				rank, offset[0], count[0]);
	for(i = 1; i < ndims; i++){
		count[i] = dimlens[i];
		offset[i] = 0;
		debug_printf("process %d, offset[%d] = %llu, count[%d] = %llu\n",
				rank, i, offset[i], i, count[i]);
	}

}


int define_nc_dtype(int pa_ncid, const char *name, int datatype_id, int typeid)
{
	int nctype = typeid;

	/* compound type */
	if(nctype == NC_COMPOUND){
		nctype = define_nc_compound(pa_ncid, name, datatype_id);
	}
	/* enum type */
	else if(nctype == NC_ENUM){
		nctype = define_nc_enum(pa_ncid, name, datatype_id);
	}
	/* opaque type */
	else if(nctype == NC_OPAQUE){
		nctype = define_nc_opaque(pa_ncid, name, datatype_id);
	}

	/* vlen type */
	else if(nctype == NC_VLEN){
		nctype = define_nc_vlen(pa_ncid, name, datatype_id);
	}

	return nctype;
}

int define_nc_compound(int pa_ncid, const char *name, int datatype_id)
{
	int			nc_varid, memb_dtype, mem_mtype, cmpd_memtype;
	int			nctype, ret;
	size_t		field_offset;
	int			nmembs;
	int			type_size;
	int			cmp_size = 0;
	int			type_existed = 1;
	int			i;
	char		var_name[VAR_NAME_LEN];
	char		newname[VAR_NAME_LEN];

	cmpd_field_t	*cfields;

	H5_ASSERT(cmpd_memtype = H5Tget_native_type(datatype_id, H5P_DEFAULT));
	H5_ASSERT(nmembs = H5Tget_nmembers(cmpd_memtype));

	cfields = (cmpd_field_t *)malloc(sizeof(cmpd_field_t) * nmembs);
	memset(cfields, 0, sizeof(cmpd_field_t) * nmembs);
	
	
	/* 
	 * define compound type in nc 
	 */
	field_offset = 0;
	for(i = 0; i < nmembs; i++){
		/* get member name */
		cfields[i].name = H5Tget_member_name(cmpd_memtype, i);
		ASSERT(cfields[i].name != NULL);

		/* get member data type */
		H5_ASSERT(memb_dtype = H5Tget_member_type(cmpd_memtype, i));
		nctype = get_nctype(memb_dtype);
		ASSERT(nctype != NC_NAT);
			
		cfields[i].dtype_id = define_nc_dtype(pa_ncid, cfields[i].name, memb_dtype, nctype);
		H5_ASSERT(H5Tclose(memb_dtype));

		/* get member offset */
		cfields[i].offset = H5Tget_member_offset(cmpd_memtype, i);

	}
	
	strcpy(var_name, "compound_");
	strcat(var_name, name);	/* set names of dim */
	
	H5_ASSERT(cmp_size = H5Tget_size(cmpd_memtype));

	debug_printf("defien_nc_compound: cmp_size = %d\n", cmp_size);
	 
	ret = nc_def_compound(pa_ncid, cmp_size, var_name, &nc_varid);
	
	/* the compound name type has already exited */
	if(ret == NC_ENAMEINUSE){
		int		nctype_id;
		int		base_typein, nctype_class;
		size_t	fld_offset, nc_cmpdsize, nc_nmembs;
		char	fld_name[VAR_NAME_LEN];

		NC_ASSERT(nc_inq_typeid(pa_ncid, var_name, &nctype_id));
		NC_ASSERT(nc_inq_user_type(pa_ncid, nctype_id, NULL, &nc_cmpdsize, &base_typein, &nc_nmembs, &nctype_class));
		
		if(nctype_class == NC_COMPOUND && nc_cmpdsize == cmp_size && nc_nmembs == nmembs){
			
			nc_varid = nctype_id;

			for(i = 0; i < nc_nmembs; i++){
				NC_ASSERT(nc_inq_compound_fieldname(pa_ncid, nctype_id, i, fld_name));
				NC_ASSERT(nc_inq_compound_fieldoffset(pa_ncid, nctype_id, i, &fld_offset));
				if(strcmp(fld_name, cfields[i].name) != 0 || fld_offset != cfields[i].offset){
					type_existed = 0;
					i = nc_nmembs;
					strcat(var_name, get_suffix());
					NC_ASSERT(nc_def_compound(pa_ncid, cmp_size, var_name, &nc_varid));
				}
			}
		}
		else{
			type_existed = 0;
			strcat(var_name, get_suffix());
			NC_ASSERT(nc_def_compound(pa_ncid, cmp_size, var_name, &nc_varid));
		}
	}
	else{
		//ASSERT(ret == 0);
		type_existed = 0;
	}

	/*
	 * insert filed
	 */
	if(type_existed == 0){
		for(i = 0; i < nmembs; i++){
			
			/* remove the illegal character */
			memset(newname, 0, VAR_NAME_LEN);
			strcpy(newname, cfields[i].name);
			format_name(newname);	
			
			/* general situation */
			if(cfields[i].dtype_id != NC_CHAR && cfields[i].dtype_id != TO_NC_ARRAY)
				NC_ASSERT(nc_insert_compound(pa_ncid, nc_varid, newname, cfields[i].offset, cfields[i].dtype_id));

			/* a array type in compound data */
			else if(cfields[i].dtype_id == TO_NC_ARRAY){
				int		super_nctype, super_dtype;
				int		super_ndims, j;
				hsize_t	super_size[DIM_SIZE];
				int		array_size[DIM_SIZE];

				H5_ASSERT(memb_dtype = H5Tget_member_type(datatype_id, i));
				H5_ASSERT(super_dtype = H5Tget_super(memb_dtype));
				
				H5_ASSERT(super_ndims = H5Tget_array_dims2(memb_dtype, super_size));
				ASSERT(super_ndims <= DIM_SIZE);
				for(j = 0; j < super_ndims; j++)
					array_size[j] = (int)super_size[j];

				super_nctype = get_nctype(super_dtype);
				super_nctype = define_nc_dtype(pa_ncid, cfields[i].name, super_dtype, super_nctype);
				
				NC_ASSERT(nc_insert_array_compound(pa_ncid, nc_varid, newname, 
					cfields[i].offset, super_nctype, super_ndims, array_size));
				
				H5_ASSERT(H5Tclose(memb_dtype));
				H5_ASSERT(H5Tclose(super_dtype));
			}

			/* a fixed length string in compound data */
			else{
				H5_ASSERT(memb_dtype = H5Tget_member_type(datatype_id, i));
				H5_ASSERT(mem_mtype = H5Tget_native_type(memb_dtype, H5P_DEFAULT));
				H5_ASSERT(type_size = H5Tget_size(mem_mtype));
				NC_ASSERT(nc_insert_array_compound(pa_ncid, nc_varid, newname, 
								cfields[i].offset, cfields[i].dtype_id, 1, &type_size));
				H5_ASSERT(H5Tclose(mem_mtype));
				H5_ASSERT(H5Tclose(memb_dtype));
			}
			free(cfields[i].name);
		}
	}

	free(cfields);

	H5_ASSERT(H5Tclose(cmpd_memtype));
	return nc_varid;
}

int define_nc_enum(int pa_ncid, const char *name, int datatype_id)
{
	int		nc_varid, field_type, memb_type;
	int		nmembs;
	char	*field_name;
	char	var_name[VAR_NAME_LEN];
	unsigned int i;
	long long field_value;
	
	/*
	 * define enum
	 */
	memset(var_name, 0, VAR_NAME_LEN);
	strcpy(var_name, "enum_");
	strcat(var_name, name);	/* set names of dim */

	H5_ASSERT(memb_type = H5Tget_super(datatype_id));	
	field_type = get_nctype(memb_type);
	ASSERT(field_type != NC_NAT);
	NC_ASSERT(nc_def_enum(pa_ncid, field_type, var_name, &nc_varid));

	/*
	 * insert member
	 */
	H5_ASSERT(nmembs = H5Tget_nmembers(datatype_id));
	for(i = 0; i < nmembs; i++){
		field_name = H5Tget_member_name(datatype_id, i);
		ASSERT(field_name != NULL);
		H5_ASSERT(H5Tget_member_value(datatype_id, i, &field_value));
		/* convert filetype to native integer type */
		H5Tconvert(memb_type, H5T_NATIVE_LLONG, 1, &field_value, NULL, H5P_DEFAULT);
		debug_printf("field value: %llu\n", field_value);
		NC_ASSERT(nc_insert_enum(pa_ncid, nc_varid, field_name, &field_value));
		free(field_name);
	}


	H5Tclose(memb_type);
	return nc_varid;
}

int define_nc_opaque(int pa_ncid, const char *name, int datatype_id)
{
	int		nc_varid;
	size_t	var_size;
	char	var_name[VAR_NAME_LEN];

	strcpy(var_name, "opaque_");
	strcat(var_name, name);	/* set names of dim */


	H5_ASSERT(var_size = H5Tget_size(datatype_id));

	NC_ASSERT(nc_def_opaque(pa_ncid, var_size, var_name, &nc_varid));

	return nc_varid;
}

int define_nc_vlen(int pa_ncid, const char *name, int datatype_id)
{
	int		nc_varid, field_type, memb_type;
	char	var_name[VAR_NAME_LEN];
	
	/*
	 * define vlen 
	 */
	strcpy(var_name, "vlen_");
	strcat(var_name, name);	/* set names of dim */

	H5_ASSERT(memb_type = H5Tget_super(datatype_id));	
	
	field_type = get_nctype(memb_type);
	assert(field_type != NC_NAT);

	field_type = define_nc_dtype(pa_ncid, name, memb_type, field_type);

	NC_ASSERT(nc_def_vlen(pa_ncid, var_name, field_type, &nc_varid));

	return nc_varid;
}


void write_nc_var(int dataset_id, int pa_ncid, const char *name, int datatype_id, nc_type nctype)
{
	int			space, space_type, ncvar, ms_id, memtype_id, xfer_plist;
	int			dimids[DIM_SIZE];
	int			ndims, i, carry;
	hsize_t		dimlens[DIM_SIZE];
	char		dimnames[DIM_SIZE][DIM_NAME_LEN];
	size_t		type_size;
	size_t		cnt[DIM_SIZE], oft[DIM_SIZE];
	hsize_t		bufsize, rd_bufsize, elmtno, all_elmts, rd_elmts;
	hsize_t		count[DIM_SIZE], rd_count[DIM_SIZE], tmp_count[DIM_SIZE], zero[DIM_SIZE];
	hsize_t		offset[DIM_SIZE], tmp_offset[DIM_SIZE];
	hsize_t		max_size = MAX_BUF_SIZE;

	//max_size *= 4;
	trans_attr_t	tattr;
	unsigned char	*databuf = NULL;

	if(nctype == NC_CHAR){
		write_nc_char(dataset_id, pa_ncid, name, datatype_id, nctype);	
		return;
	}

	if(nctype == TO_NC_ARRAY){
		write_nc_array(dataset_id, pa_ncid, name, datatype_id, nctype);	
		return;
	}	
	/*
	 * get data space info
	 */
	H5_ASSERT(space = H5Dget_space(dataset_id));
	H5_ASSERT(space_type = H5Sget_simple_extent_type(space));

	/* get dimension info */
	H5_ASSERT(ndims = H5Sget_simple_extent_dims(space, dimlens, NULL));
	ASSERT(ndims <= DIM_SIZE);
	
	/* 
	 * define dimension and variable in nc
	 */
	for(i = 0; i < ndims; i++){
		memset(dimnames[i], 0, DIM_NAME_LEN);
		strcpy(dimnames[i], name);
		strcat(dimnames[i], cords[i]);	/* set names of dim */
		NC_ASSERT(nc_def_dim(pa_ncid, dimnames[i], dimlens[i], &(dimids[i])));
	}
			
	NC_ASSERT(nc_def_var(pa_ncid, name, nctype, ndims, dimids, &ncvar));

	/*
	 * if type is opaque, write the tag of hdf as a attr of nc
	 */
	if(H5Tget_class(datatype_id) == H5T_OPAQUE){
		char	*tag;
		size_t	len;
		tag = H5Tget_tag(datatype_id);
		ASSERT(tag != NULL);
		len = strlen(tag);
		NC_ASSERT(nc_put_att(pa_ncid, ncvar, "tag", NC_CHAR, len, tag));
		free(tag);
	}

	/*
	 * write attribute
	 */
	tattr.pa_ncid = pa_ncid;
	tattr.varid = ncvar;	
	tattr.type = DATASET;
	tattr.variable = 0;
	H5Aiterate(dataset_id, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, conv_attribute_cb, &tattr);

	/*
	 * read data from hdf5
	 */

	if(space_type == H5S_NULL || space_type == H5S_NO_CLASS){

		my_syslog(LOG_WARNING, "ph5toNC: WARNING--meeting a empty dataset%s\n", name);

		H5_ASSERT(H5Sclose(space));
		return;
	}

	/* set the hyperslab for every process */

	set_hyperslab(count, offset, dimlens, ndims);

	H5_ASSERT(memtype_id = H5Tget_native_type(datatype_id, H5T_DIR_DEFAULT));	
		
	/* allocate mem for databuf */
	type_size = MAX(H5Tget_size(memtype_id), H5Tget_size(datatype_id));
	bufsize = type_size;
	all_elmts = 1;
	for(i = 0; i< ndims; i++){
		bufsize *= count[i];
		all_elmts *= count[i];
	}
	/* test for multiple processes */
	debug_printf("bufsize = %llu\n", bufsize);

	/* 
	 * set the bufsize for each read 
	 */
	rd_bufsize = type_size;
	for(i = ndims-1; i >= 0; i--){
		hsize_t tmp_size = max_size / rd_bufsize;
		if(tmp_size == 0)	/* type_size > MAX_BUF_SIZE */
		  tmp_size = 1;
		rd_count[i] = MIN(count[i], tmp_size);
		rd_bufsize *= rd_count[i];
		ASSERT(rd_bufsize > 0);

		tmp_offset[i] = offset[i];
	}

	/* check for overflow */
	ASSERT(rd_bufsize == (hsize_t)((size_t)rd_bufsize));

	/* test for multiple processes */
	debug_printf("rd_bufsize = %llu\n", rd_bufsize);

	databuf = (unsigned char *)malloc((size_t)rd_bufsize);
	//ASSERT(databuf);
	if(databuf == NULL){
		printf("memory allocated error!\n");
		exit(EXIT_CODE);
	}

	H5_ASSERT(ms_id = H5Screate_simple(ndims, count, NULL));
	memset(zero, 0, sizeof(zero));
	//memset(tmp_offset, 0, sizeof(tmp_offset));

#ifndef OPTIMIZE
	memset(databuf, 0, (size_t)rd_bufsize);
#endif
	/* set the collective transfer properties list */
	H5_ASSERT(xfer_plist = H5Pcreate(H5P_DATASET_XFER));
	H5_ASSERT(H5Pset_dxpl_mpio(xfer_plist, H5FD_MPIO_COLLECTIVE));
	NC_ASSERT(nc_var_par_access(pa_ncid, ncvar, NC_COLLECTIVE));
	
	for(elmtno = 0; elmtno < all_elmts; elmtno += rd_elmts){
		if(ndims > 0){
			for(i = 0, rd_elmts = 1; i < ndims; i++){
				tmp_count[i] = MIN(count[i] + offset[i] - tmp_offset[i], rd_count[i]);
				rd_elmts *= tmp_count[i];
			}
			if((rd_elmts * type_size) < rd_bufsize){
				free(databuf);
				databuf = (unsigned char *)malloc((size_t)(rd_elmts * type_size));
			}
			H5_ASSERT(H5Sselect_hyperslab(space, H5S_SELECT_SET, 
							tmp_offset, NULL, tmp_count, NULL));
			H5_ASSERT(H5Sselect_hyperslab(ms_id, H5S_SELECT_SET, 
							zero, NULL, tmp_count, NULL));
		}
		else{
			H5_ASSERT(H5Sselect_all(space));
			H5_ASSERT(H5Sselect_all(ms_id));
			rd_elmts = 1;
		}
		
		H5_ASSERT(H5Dread(dataset_id, memtype_id, ms_id, 
						space, xfer_plist, databuf));
		/*
		 * write data in nc
		 */
		for(i = 0; i < ndims; i++){
			cnt[i] = (size_t)tmp_count[i];
			oft[i] = (size_t)tmp_offset[i];
		}
		NC_ASSERT(nc_put_vara(pa_ncid, ncvar, oft, cnt, databuf));

		debug_printf("write data in nc: %lld\n", rd_elmts);

		/* calculate the next hyperslab offset */
		for(i = (ndims -1), carry = 1; (i >= 0) & carry; i--){
			tmp_offset[i] += tmp_count[i];
			if(tmp_offset[i] == count[i])
			  tmp_offset[i] = offset[i];
			else
			  carry = 0;
		}

	}


	H5_ASSERT(H5Pclose(xfer_plist));
	H5_ASSERT(H5Tclose(memtype_id));
	H5_ASSERT(H5Sclose(space));
	H5_ASSERT(H5Sclose(ms_id));

	
	
	/*
	 * release
	 */
	free(databuf);

	return;
}


void write_nc_char(int dataset_id, int pa_ncid, const char *name, int datatype_id, nc_type nctype)
{
	int			space, space_type, ncvar, memtype_id, xfer_plist;
	int			dimids[DIM_SIZE];
	int			ndims, i;
	hsize_t		dimlens[DIM_SIZE];
	hsize_t		bufsize;
	char		dimnames[DIM_SIZE][DIM_NAME_LEN];
	size_t		typesize;
	size_t		cnt[DIM_SIZE], oft[DIM_SIZE];
	trans_attr_t	tattr;
	unsigned char*	databuf = NULL;
	
	/*
	 * get data space info
	 */
	H5_ASSERT(space = H5Dget_space(dataset_id));
	H5_ASSERT(space_type = H5Sget_simple_extent_type(space));

	/* get dimension info */
	H5_ASSERT(ndims = H5Sget_simple_extent_dims(space, dimlens, NULL));
	ASSERT(ndims <= DIM_SIZE);

	/* allocate mem for databuf */
	H5_ASSERT(typesize = H5Tget_size(datatype_id));
	typesize++;		/* make room for null terminator */
	
	/* 
	 * define variable in nc
	 */
	for(i = 0; i < ndims; i++){
		strcpy(dimnames[i], name);
		strcat(dimnames[i], cords[i]);	/* set names of dim */
		NC_ASSERT(nc_def_dim(pa_ncid, dimnames[i], dimlens[i], &(dimids[i])));
	}
	strcpy(dimnames[ndims], name);
	strcat(dimnames[ndims], cords[ndims]);	/* set names of dim */
	NC_ASSERT(nc_def_dim(pa_ncid, dimnames[ndims], typesize, &(dimids[ndims])));
	
	NC_ASSERT(nc_def_var(pa_ncid, name, nctype, ndims+1, dimids, &ncvar));

	/*
	 * write attribute
	 */
	tattr.pa_ncid = pa_ncid;
	tattr.varid = ncvar;	
	tattr.type = DATASET;
	tattr.variable = 0;
	H5Aiterate(dataset_id, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, conv_attribute_cb, &tattr);

	/*
	 * meet a NULL dataspace
	 */
	if(space_type == H5S_NULL || space_type == H5S_NO_CLASS){

		my_syslog(LOG_WARNING, "ph5toNC: WARNING--meeting a empty dataset%s\n", name);

		H5_ASSERT(H5Sclose(space));
		return;
	}
	
	/*
	 * read data from HDF5
	 */
	H5_ASSERT(memtype_id = H5Tcopy(H5T_C_S1));	/* get the corresponding memtype */
	H5_ASSERT(H5Tset_size(memtype_id, typesize));

	bufsize = typesize;
	for(i = 0; i < ndims; i++)
		bufsize *= (size_t)dimlens[i]; 
	ASSERT(bufsize == (hsize_t)((size_t)bufsize));
	databuf = (unsigned char *)malloc((size_t)bufsize);
	//ASSERT(databuf);
	if(databuf == NULL){
		printf("memory allocated error!\n");
		exit(EXIT_CODE);
	}

	/* test for multiple processes */
	debug_printf("bufsize = %llu\n", bufsize);
	
	/* set the collective transfer properties list */
	H5_ASSERT(xfer_plist = H5Pcreate(H5P_DATASET_XFER));
	H5_ASSERT(H5Pset_dxpl_mpio(xfer_plist, H5FD_MPIO_COLLECTIVE));
	H5_ASSERT(H5Dread(dataset_id, memtype_id, H5S_ALL, H5S_ALL, xfer_plist, databuf));

	/*
	 * release
	 */
	H5_ASSERT(H5Tclose(memtype_id));
	H5_ASSERT(H5Pclose(xfer_plist));
	H5_ASSERT(H5Sclose(space));


	for(i = 0; i < ndims; i++){
		oft[i] = 0;
		cnt[i] = dimlens[i];
	}
	oft[ndims] = 0;
	cnt[ndims] = typesize;
	NC_ASSERT(nc_var_par_access(pa_ncid, ncvar, NC_COLLECTIVE));
	NC_ASSERT(nc_put_vara(pa_ncid, ncvar, oft, cnt, databuf));

	free(databuf);
	return;
}

void write_nc_array(int dataset_id, int pa_ncid, const char *name, int datatype_id, nc_type nctype)
{
	int			space, space_type, ncvar, ms_id, memtype_id, xfer_plist, super_dtype, super_memtype;
	int			dimids[DIM_SIZE];
	int			ndims, array_ndims, sum_ndims, i;
	hsize_t		dimlens[DIM_SIZE], array_dimlens[DIM_SIZE];
	char		dimnames[DIM_SIZE][DIM_NAME_LEN];
	size_t		type_size;
	size_t		cnt[DIM_SIZE], oft[DIM_SIZE];
	hsize_t		bufsize;
	hsize_t		count[DIM_SIZE];
	hsize_t		offset[DIM_SIZE];
	
	trans_attr_t	tattr;
	unsigned char	*databuf = NULL;

	/*
	 * get data space info
	 */
	H5_ASSERT(space = H5Dget_space(dataset_id));
	H5_ASSERT(space_type = H5Sget_simple_extent_type(space));

	/* get dimension info */
	H5_ASSERT(ndims = H5Sget_simple_extent_dims(space, dimlens, NULL));
	H5_ASSERT(array_ndims = H5Tget_array_dims2(datatype_id, array_dimlens));
	sum_ndims = ndims + array_ndims;
	ASSERT(sum_ndims <= DIM_SIZE);

	
	for(i = ndims; i < sum_ndims; i++)
		dimlens[i] = array_dimlens[i-ndims];

	H5_ASSERT(super_dtype = H5Tget_super(datatype_id));
	nctype = get_nctype(super_dtype);
	//ASSERT(nctype != TO_NC_ARRAY);
	if(nctype == TO_NC_ARRAY){
		printf("can't handle ARRAY in ARRAY!\n");
		exit(EXIT_CODE);
	}

	/* 
	 * define dimension and variable in nc
	 */
	for(i = 0; i < sum_ndims; i++){
		memset(dimnames[i], 0, DIM_NAME_LEN);
		strcpy(dimnames[i], name);
		strcat(dimnames[i], cords[i]);	/* set names of dim */
		NC_ASSERT(nc_def_dim(pa_ncid, dimnames[i], dimlens[i], &(dimids[i])));
	}
	
	nctype = define_nc_dtype(pa_ncid, name, super_dtype, nctype);
	NC_ASSERT(nc_def_var(pa_ncid, name, nctype, sum_ndims, dimids, &ncvar));

	/*
	 * write attribute
	 */
	tattr.pa_ncid = pa_ncid;
	tattr.varid = ncvar;	
	tattr.type = DATASET;
	tattr.variable = 0;
	H5Aiterate(dataset_id, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, conv_attribute_cb, &tattr);


	/*
	 * NULL space
	 */
	if(space_type == H5S_NULL || space_type == H5S_NO_CLASS){

		my_syslog(LOG_WARNING, "ph5toNC: WARNING--meeting a empty dataset%s\n", name);

		H5_ASSERT(H5Sclose(space));
		return;
	}

	/*
	 * read data from hdf5
	 */

	/* set the hyperslab of file space and memory space for each process */
	set_hyperslab(count, offset, dimlens, ndims);
	H5_ASSERT(H5Sselect_hyperslab(space, H5S_SELECT_SET, offset, NULL, count, NULL));
	H5_ASSERT(ms_id = H5Screate_simple(ndims, count, NULL));

	/* get the super type of array */	
	H5_ASSERT(memtype_id = H5Tget_native_type(datatype_id, H5T_DIR_DEFAULT));	
	H5_ASSERT(super_memtype = H5Tget_native_type(super_dtype, H5T_DIR_DEFAULT));

	/* allocate mem for databuf */
	type_size = MAX(H5Tget_size(super_memtype), H5Tget_size(super_dtype));
	bufsize = type_size;
	
	for(i = 0; i < array_ndims; i++)
		bufsize *= array_dimlens[i];

	for(i = 0; i< ndims; i++)
		bufsize *= count[i];
	/* test the if there is data overflow */
	ASSERT(bufsize == (hsize_t)((size_t)bufsize));

	/* test for multiple processes */
	debug_printf("bufsize = %llu\n", bufsize);
	
	databuf = (unsigned char *)malloc((size_t)bufsize);
	//ASSERT(databuf);
	if(databuf == NULL){
		printf("memory allocate error!\n");
		exit(EXIT_CODE);
	}

	memset(databuf, 0, bufsize);
		
	/* set the collective transfer properties list */
	H5_ASSERT(xfer_plist = H5Pcreate(H5P_DATASET_XFER));
	H5_ASSERT(H5Pset_dxpl_mpio(xfer_plist, H5FD_MPIO_COLLECTIVE));
	H5_ASSERT(H5Dread(dataset_id, memtype_id, ms_id, space, xfer_plist, databuf));
	//H5_ASSERT(H5Dread(dataset_id, memtype_id, H5S_ALL, H5S_ALL, H5P_DEFAULT, databuf));

	H5_ASSERT(H5Tclose(super_dtype));
	H5_ASSERT(H5Tclose(super_memtype));
	H5_ASSERT(H5Pclose(xfer_plist));
	H5_ASSERT(H5Tclose(memtype_id));
	H5_ASSERT(H5Sclose(space));
	H5_ASSERT(H5Sclose(ms_id));

	/*
	 * write data in nc
	 */
	for(i = 0; i < ndims; i++){
		ASSERT(count[i] == (hsize_t)((size_t)count[i]));
		ASSERT(offset[i] == (hsize_t)((size_t)offset[i]));
		cnt[i] = (size_t)count[i];
		oft[i] = (size_t)offset[i];
	}
	for(i = ndims; i < sum_ndims; i++){
		ASSERT(dimlens[i] == (hsize_t)((size_t)dimlens[i]));
		cnt[i] = (size_t)dimlens[i];
		oft[i] = 0;
	}
	NC_ASSERT(nc_var_par_access(pa_ncid, ncvar, NC_COLLECTIVE));
	NC_ASSERT(nc_put_vara(pa_ncid, ncvar, oft, cnt, databuf));

	/*
	 * release
	 */
	free(databuf);

	return;

}

void write_nc_attr(const char *name, int attr_id, int type, int nctype, trans_attr_t *data_p)
{
	int				mem_type, space, space_type, ncvar;
	int				ndims, i;
	unsigned char	*buf;
	hsize_t			size[DIM_SIZE], alloc_size, nelmts = 1;

	if(nctype == NC_CHAR){
		write_char_attr(name, attr_id, type, nctype, data_p);
		return;
	}
	if(nctype == TO_NC_ARRAY){
		write_array_attr(name, attr_id, type, nctype, data_p);
		return;

	}	
	/*
	 * read raw data from h5
	 */
	H5_ASSERT(space = H5Aget_space(attr_id));
	H5_ASSERT(space_type = H5Sget_simple_extent_type(space));

	if(space_type == H5S_NULL || space_type == H5S_NO_CLASS){

		my_syslog(LOG_WARNING, "ph5toNC: WARNING--meeting a empty attribute %s\n", name);

		H5_ASSERT(H5Sclose(space));
		return;
	}
	memset(size, 0, DIM_SIZE * sizeof(hsize_t));
	
	H5_ASSERT(ndims = H5Sget_simple_extent_dims(space, size, NULL));
	
	for(i = 0; i < ndims; i++)
	  nelmts *= size[i];

	H5_ASSERT(mem_type = H5Tget_native_type(type, H5T_DIR_DEFAULT));
	alloc_size = nelmts * MAX(H5Tget_size(type), H5Tget_size(mem_type));
	ASSERT(alloc_size == (hsize_t)((size_t)alloc_size));
	debug_printf("alloc_size: %llu\n", alloc_size);
	buf = (unsigned char *)malloc((size_t)alloc_size);
	//ASSERT(buf);
	if(buf == NULL){
		printf("memory allocate error");
		exit(EXIT_CODE);
	}

	H5_ASSERT(H5Aread(attr_id, mem_type, buf));

	/*
	 * write attribute to nc
	 */
	if(data_p->type == GROUP || data_p->type == NAMED_DATATYPE)
		ncvar = NC_GLOBAL;
	else
		ncvar = data_p->varid;

	ASSERT(nelmts == (hsize_t)((size_t)nelmts));
	NC_ASSERT(nc_put_att(data_p->pa_ncid, ncvar, name, nctype, (size_t)nelmts, buf));

	/*
	 * if type is opaque, write the tag of hdf as a attr of nc
	 */
	if(H5Tget_class(type) == H5T_OPAQUE){
		char	*tag;
		size_t	len;
		tag = H5Tget_tag(type);
		ASSERT(tag != NULL);
		len = strlen(tag);
		NC_ASSERT(nc_put_att(data_p->pa_ncid, ncvar, "tag", NC_CHAR, len, tag));
		free(tag);
	}

	/*
	 * release
	 */
#if 0
	//TODO: realease the compound which has variable length datatype
	if(data_p->variable == 1){
		H5_ASSERT(H5Dvlen_reclaim(mem_type, space, H5P_DEFAULT, buf));
	}
#endif
	free(buf);
	H5_ASSERT(H5Tclose(mem_type));
	H5_ASSERT(H5Sclose(space));
	
	return;
}

void write_char_attr(const char *name, int attr_id, int type, int nctype, trans_attr_t *data_p)
{
	int		mem_type, space, space_type, ncvar, memtype;
	int		ndims, i;
	char	**buf;
	hsize_t	size[DIM_SIZE], alloc_size, nelmts = 1;
	size_t	usize;

	H5_ASSERT(space = H5Aget_space(attr_id));
	H5_ASSERT(space_type = H5Sget_simple_extent_type(space));

	if(space_type == H5S_NULL || space_type == H5S_NO_CLASS){

		my_syslog(LOG_WARNING, "ph5toNC: WARNING--meeting a empty attribute %s\n", name);

		H5_ASSERT(H5Sclose(space));
		return;
	}
	memset(size, 0, DIM_SIZE * sizeof(hsize_t));
	
	H5_ASSERT(ndims = H5Sget_simple_extent_dims(space, size, NULL));

	for(i = 0; i < ndims; i++)
	  nelmts *= size[i];
	
	buf = (char **)malloc(nelmts * sizeof(char *));
	//ASSERT(buf != NULL);
	if(buf == NULL){
		printf("memory allocate error!\n");
		exit(EXIT_CODE);
	}

	H5_ASSERT(mem_type = H5Tget_native_type(type, H5T_DIR_DEFAULT));
	usize = MAX(H5Tget_size(type), H5Tget_size(mem_type));
	usize++;

	debug_printf("write_char_attr: usize = %zd\n", usize);

	alloc_size = nelmts * usize;
	ASSERT(alloc_size == (hsize_t)((size_t)alloc_size));

	debug_printf("write_char_attr: alloc_size: %llu\n", alloc_size);

	buf[0] = (char *)malloc((size_t)alloc_size);
	//ASSERT(buf[0]);
	if(buf == NULL){
		printf("memory allocate error!\n");
		exit(EXIT_CODE);
	}

	H5_ASSERT(memtype = H5Tcopy(H5T_C_S1));
	H5_ASSERT(H5Tset_size(memtype, usize));
	H5_ASSERT(H5Aread(attr_id, memtype, buf[0]));

	if(ndims > 0){
		for(i = 1; i < nelmts; i++){
			buf[i] = buf[0] + i * usize;
		}
	}

	/*
	 * write attribute to nc
	 */
	nctype = NC_STRING;
	
	if(data_p->type == GROUP || data_p->type == NAMED_DATATYPE)
		ncvar = NC_GLOBAL;
	else 
		ncvar = data_p->varid;
	
	NC_ASSERT(nc_put_att(data_p->pa_ncid, ncvar, name, nctype, (size_t)nelmts, buf));

	/*
	 * release
	 */
	free(buf[0]);
	free(buf);
	H5_ASSERT(H5Tclose(memtype));
	H5_ASSERT(H5Tclose(mem_type));
	H5_ASSERT(H5Sclose(space));
	
	return;
}

void write_array_attr(const char *name, int attr_id, int type, int nctype, trans_attr_t *data_p)
{
	int				mem_type, space, space_type, ncvar;
	int				super_dtype, super_memtype;
	int				ndims, array_ndims, sum_ndims, i;
	unsigned char	*buf;
	hsize_t			size[DIM_SIZE], array_size[DIM_SIZE], alloc_size, nelmts = 1;

	/*
	 * read raw data from h5
	 */
	H5_ASSERT(space = H5Aget_space(attr_id));
	H5_ASSERT(space_type = H5Sget_simple_extent_type(space));

	if(space_type == H5S_NULL || space_type == H5S_NO_CLASS){
		
		my_syslog(LOG_WARNING, "ph5toNC: WARNING--meeting a empty attribute %s\n", name);
		
		H5_ASSERT(H5Sclose(space));
		return;
	}
	memset(size, 0, DIM_SIZE * sizeof(hsize_t));

	/* get dimension info */
	H5_ASSERT(ndims = H5Sget_simple_extent_dims(space, size, NULL));
	H5_ASSERT(array_ndims = H5Tget_array_dims2(type, array_size));
	sum_ndims = ndims + array_ndims;
	ASSERT(sum_ndims <= DIM_SIZE);
	
	for(i = ndims; i < sum_ndims; i++)
		size[i] = array_size[i-ndims];

	H5_ASSERT(super_dtype = H5Tget_super(type));
	nctype = get_nctype(super_dtype);
	ASSERT(nctype != TO_NC_ARRAY);

	for(i = 0; i < sum_ndims; i++)
	  nelmts *= size[i];

	H5_ASSERT(mem_type = H5Tget_native_type(type, H5T_DIR_DEFAULT));
	H5_ASSERT(super_memtype = H5Tget_native_type(super_dtype, H5T_DIR_DEFAULT));

	alloc_size = nelmts * MAX(H5Tget_size(super_dtype), H5Tget_size(super_memtype));
	ASSERT(alloc_size == (hsize_t)((size_t)alloc_size));
	debug_printf("alloc_size: %llu\n", alloc_size);
	buf = (unsigned char *)malloc((size_t)alloc_size);
	//ASSERT(buf);
	if(buf == NULL){
		printf("memory allocate error!\n");
		exit(EXIT_CODE);
	}

	H5_ASSERT(H5Aread(attr_id, mem_type, buf));

	/*
	 * write attribute to nc
	 */
	if(data_p->type == GROUP || data_p->type == NAMED_DATATYPE)
		ncvar = NC_GLOBAL;
	else
		ncvar = data_p->varid;

	ASSERT(nelmts == (hsize_t)((size_t)nelmts));

	nctype = define_nc_dtype(data_p->pa_ncid, name, super_dtype, nctype);
	NC_ASSERT(nc_put_att(data_p->pa_ncid, ncvar, name, nctype, (size_t)nelmts, buf));

	/*
	 * release
	 */
	free(buf);
	H5_ASSERT(H5Tclose(super_memtype));
	H5_ASSERT(H5Tclose(super_dtype));
	H5_ASSERT(H5Tclose(mem_type));
	H5_ASSERT(H5Sclose(space));
	
	return;
}

void serial_trans(char *h5path, char *ncpath)
{
	int h5file, ncfile;
		
	/* open the h5 file*/
    H5_ASSERT(h5file = H5Fopen(h5path, H5F_ACC_RDONLY, H5P_DEFAULT));	

	NC_ASSERT(nc_open(ncpath, NC_WRITE, &ncfile));

	init_visited();

	serial_conv_group(h5file, ncfile, "/");

    H5_ASSERT(H5Fclose(h5file));

    NC_ASSERT(nc_close(ncfile));

	return;
}

int serial_conv_group(int pa_h5id, int pa_ncid, const char *name)
{
	int				gid;
	int				new_ncid;
	trans_data_t	tdata;
	H5O_info_t		objinfo;
	char			newname[VAR_NAME_LEN];
	
	tdata.pa_ncid = pa_ncid;
	tdata.prefix = name;

	/* open a group */
	H5_ASSERT(gid = H5Gopen2(pa_h5id, name, H5P_DEFAULT));


	/* get the group infomation */
    H5_ASSERT(H5Oget_info(gid, &objinfo));

	if(check_visited(objinfo.addr) == 0){
		set_visited(objinfo.addr);
		/* open a group in nc */
		if(strcmp(name, "/")){
			
			/* remove the illegal character */
			memset(newname, 0, VAR_NAME_LEN);
			strcpy(newname, name);
			format_name(newname);			
			
			NC_ASSERT(nc_inq_ncid(pa_ncid, newname, &new_ncid));
			tdata.pa_ncid = new_ncid;
		}

		H5Literate(gid, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, serial_conv_all_cb, &tdata);
	}

	H5_ASSERT(H5Gclose(gid));
	return 0;
}

int serial_conv_dataset(int pa_h5id, int pa_ncid, const char *name)
{
	int				dataset_id, datatype_id;
	H5O_info_t		objinfo;
	nc_type			nctype;
	char			newname[VAR_NAME_LEN];

	/* open dataset */
	H5_ASSERT(dataset_id = H5Dopen1(pa_h5id, name));
	
	/* get information of dataset */
	H5_ASSERT(H5Oget_info(dataset_id, &objinfo));

	/* 
	 * read the data from the h5 file, then put these data into a nc variable 
	 */
	if(check_visited(objinfo.addr) == 0){	
		
		set_visited(objinfo.addr);

		H5_ASSERT(datatype_id = H5Dget_type(dataset_id));	
		
		nctype = get_nctype(datatype_id);			
	
		/*
		 * variable type
		 */
		if(check_unvst_addr(objinfo.addr)){
		
			/* remove the illegal character */
			memset(newname, 0, VAR_NAME_LEN);
			strcpy(newname, name);
			format_name(newname);

			/* string */
			if(nctype == NC_STRING){
				serial_write_string(dataset_id, pa_ncid, newname, datatype_id, nctype);
			}
	
			/* vlen */
			else if(nctype == NC_VLEN){
				nctype = define_nc_vlen(pa_ncid, newname, datatype_id);		
				serial_write_vlen(dataset_id, pa_ncid, newname, datatype_id, nctype);
			}
			
			/* compound */
			else if(nctype == NC_COMPOUND){
				nctype = define_nc_compound(pa_ncid, newname, datatype_id);
				serial_write_compound(dataset_id, pa_ncid, newname, datatype_id, nctype);
			}

			/* array */
			else if(nctype == TO_NC_ARRAY){
				serial_write_array(dataset_id, pa_ncid, newname, datatype_id, nctype);
			}
		}

		H5_ASSERT(H5Tclose(datatype_id));
	}/* end if(check_visited()) */

	H5_ASSERT(H5Dclose(dataset_id));
	return 0;
}

int serial_conv_all_cb(hid_t id, const char * name, const H5L_info_t *linfo, void *data)
{
	
    H5O_info_t		objinfo;
	trans_data_t	*tdata_p = (trans_data_t *) data;

	const char*			prefix = tdata_p->prefix;
	char*			fullname;

    fullname = (char *)malloc(strlen(prefix) + strlen(name) + 2);
	sprintf(fullname, "%s/%s", prefix, name);
	
	H5_ASSERT(H5Oget_info(id, &objinfo));
    
	/* get object info */
	if(linfo->type == H5L_TYPE_HARD){   
        H5_ASSERT(H5Oget_info_by_name(id, name, &objinfo, H5P_DEFAULT));

        switch(objinfo.type){
			
		case H5O_TYPE_GROUP:

			my_syslog(LOG_NOTICE, "ph5toNC: start group %s\n", fullname);

            serial_conv_group(id, tdata_p->pa_ncid, name);

			my_syslog(LOG_NOTICE, "ph5toNC: end group %s\n", fullname);

			break;

        case H5O_TYPE_DATASET:

			my_syslog(LOG_NOTICE, "ph5toNC: start dataset %s\n", fullname);

			serial_conv_dataset(id, tdata_p->pa_ncid, name);

			my_syslog(LOG_NOTICE, "ph5toNC: end dataset %s\n", fullname);

			break;

		case H5O_TYPE_NAMED_DATATYPE:
			break;

		case H5O_TYPE_UNKNOWN:
			break;

		case H5O_TYPE_NTYPES:
			break;

        default:
			ASSERT(0);
			break;
        }
    }

	free(fullname);

	return 0;
}

void serial_write_compound(int dataset_id, int pa_ncid, const char *name, int datatype_id, nc_type nctype)
{
	int			space, space_type, ncvar, memtype_id;
	int			dimids[DIM_SIZE];
	int			ndims, i;
	hsize_t		dimlens[DIM_SIZE];
	char		dimnames[DIM_SIZE][DIM_NAME_LEN];
	size_t		bufsize;

	trans_attr_t	tattr;
	unsigned char	*databuf = NULL;

	/*
	 * get data space info
	 */
	H5_ASSERT(space = H5Dget_space(dataset_id));
	H5_ASSERT(space_type = H5Sget_simple_extent_type(space));

	/* get dimension info */
	H5_ASSERT(ndims = H5Sget_simple_extent_dims(space, dimlens, NULL));
	ASSERT(ndims <= DIM_SIZE);

	/* 
	 * define dimension and variable in nc
	 */

	for(i = 0; i < ndims; i++){

		memset(dimnames[i], 0, DIM_NAME_LEN);
		strcpy(dimnames[i], name);
		strcat(dimnames[i], cords[i]);			
		NC_ASSERT(nc_def_dim(pa_ncid, dimnames[i], dimlens[i], &(dimids[i])));
	}
			
	NC_ASSERT(nc_def_var(pa_ncid, name, nctype, ndims, dimids, &ncvar));


	/*
	 * write attribute
	 */
	tattr.pa_ncid = pa_ncid;
	tattr.varid = ncvar;	
	tattr.type = DATASET;
	tattr.variable = 0;
	H5Aiterate(dataset_id, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, conv_attribute_cb, &tattr);	
	
	/*
	 * if meet NULL data space
	 */
	if(space_type == H5S_NULL || space_type == H5S_NO_CLASS){

		my_syslog(LOG_WARNING, "ph5toNC: WARNING--meeting a empty dataset%s\n", name);

		H5_ASSERT(H5Sclose(space));
		return;
	}

	/*
	 * read data from hdf5
	 */	
	H5_ASSERT(memtype_id = H5Tget_native_type(datatype_id, H5T_DIR_DEFAULT));	
	
	/* allocate mem for databuf */
	bufsize = H5Tget_size(memtype_id);
	ASSERT(bufsize > 0);
	for(i = 0; i< ndims; i++)
	  bufsize *= dimlens[i];
	databuf = (unsigned char *)malloc((size_t)bufsize);
	ASSERT(databuf);

	debug_printf("serial_write_compound: compound bufsize = %zd\n", bufsize);

	H5_ASSERT(H5Dread(dataset_id, memtype_id, H5S_ALL, H5S_ALL, H5P_DEFAULT, databuf));

	/*
	 * write data in nc
	 */
	NC_ASSERT(nc_put_var(pa_ncid, ncvar, databuf));

	/*
	 * release
	 */
	H5_ASSERT(H5Dvlen_reclaim(memtype_id, space, H5P_DEFAULT, databuf));
	free(databuf);

	H5_ASSERT(H5Tclose(memtype_id));
	H5_ASSERT(H5Sclose(space));
	return;
}

void serial_write_string(int dataset_id, int pa_ncid, const char *name, int datatype_id, nc_type nctype)
{
	int			space, space_type, ncvar, memtype_id;
	int			dimids[DIM_SIZE];
	int			ndims, i;
	hsize_t		dimlens[DIM_SIZE];
	size_t		nelmts = 1;
	char		dimnames[DIM_SIZE][DIM_NAME_LEN];
	char		**databuf = NULL;
	trans_attr_t	tattr;

	/*
	 * get data space info
	 */
	H5_ASSERT(space = H5Dget_space(dataset_id));
	H5_ASSERT(space_type = H5Sget_simple_extent_type(space));

	/* get dimension info */
	H5_ASSERT(ndims = H5Sget_simple_extent_dims(space, dimlens, NULL));
	ASSERT(ndims <= DIM_SIZE);
	
	/* 
	 * define variable in nc
	 */
	for(i = 0; i < ndims; i++){

		memset(dimnames[i], 0, DIM_NAME_LEN);
		strcpy(dimnames[i], name);
		strcat(dimnames[i], cords[i]);	/* set names of dim */
		NC_ASSERT(nc_def_dim(pa_ncid, dimnames[i], (size_t)dimlens[i], &(dimids[i])));
	}

	NC_ASSERT(nc_def_var(pa_ncid, name, NC_STRING, ndims, dimids, &ncvar));

	/*
	 * write attribute
	 */
	tattr.pa_ncid = pa_ncid;
	tattr.varid = ncvar;	
	tattr.type = DATASET;
	tattr.variable = 0;
	H5Aiterate(dataset_id, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, conv_attribute_cb, &tattr);
	
	/*
	 * meet a NULL dataspace
	 */
	if(space_type == H5S_NULL || space_type == H5S_NO_CLASS){

		my_syslog(LOG_WARNING, "ph5toNC: WARNING--meeting a empty dataset%s\n", name);

		H5_ASSERT(H5Sclose(space));
		return;
	}
	
	/*
	 * read data from HDF5
	 */

	/* allocate mem for databuf */
	H5_ASSERT(memtype_id = H5Tcopy(H5T_C_S1));	/* get the corresponding memtype */
	H5_ASSERT(H5Tset_size(memtype_id, H5T_VARIABLE));

	for(i = 0; i < ndims; i++)
		nelmts *= dimlens[i];

	databuf = (char **)malloc(nelmts * sizeof(char *));
	
	H5_ASSERT(H5Dread(dataset_id, memtype_id, H5S_ALL, H5S_ALL, H5P_DEFAULT, databuf));
	
	/* write raw data */
	NC_ASSERT(nc_put_var_string(pa_ncid, ncvar, (const char **)databuf));
	
	/*
	 * release
	 */
	H5_ASSERT(H5Dvlen_reclaim(memtype_id, space, H5P_DEFAULT, databuf));
	free(databuf);

	H5_ASSERT(H5Tclose(memtype_id));
	H5_ASSERT(H5Sclose(space));

	return;
}

void serial_write_vlen(int dataset_id, int pa_ncid, const char *name, int datatype_id, nc_type nctype)
{
	int			space, space_type, ncvar, memtype_id;
	int			dimids[DIM_SIZE];
	int			ndims, i;
	hsize_t		dimlens[DIM_SIZE];
	size_t		nelmts = 1;
	char		dimnames[DIM_SIZE][DIM_NAME_LEN];
	trans_attr_t	tattr;
	hvl_t		*vlen_data = NULL;

	/*
	 * get data space info
	 */
	H5_ASSERT(space = H5Dget_space(dataset_id));
	H5_ASSERT(space_type = H5Sget_simple_extent_type(space));

	/* get dimension info */
	H5_ASSERT(ndims = H5Sget_simple_extent_dims(space, dimlens, NULL));
	ASSERT(ndims <= DIM_SIZE);

	/* 
	 * define dimension and variable in nc
	 */

	for(i = 0; i < ndims; i++){

		memset(dimnames[i], 0, DIM_NAME_LEN);
		strcpy(dimnames[i], name);
		strcat(dimnames[i], cords[i]);	/* set names of dim */
		NC_ASSERT(nc_def_dim(pa_ncid, dimnames[i], dimlens[i], &(dimids[i])));
	}
			
	NC_ASSERT(nc_def_var(pa_ncid, name, nctype, ndims, dimids, &ncvar));

	/*
	 * write attribute
	 */
	tattr.pa_ncid = pa_ncid;
	tattr.varid = ncvar;	
	tattr.type = DATASET;
	tattr.variable = 0;
	H5Aiterate(dataset_id, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, conv_attribute_cb, &tattr);

	/*
	 * meet a NULL dataspace
	 */
	if(space_type == H5S_NULL || space_type == H5S_NO_CLASS){

		my_syslog(LOG_WARNING, "ph5toNC: WARNING--meeting a empty dataset%s\n", name);

		H5_ASSERT(H5Sclose(space));
		return;
	}
	
	/*
	 * read raw data from hdf5 file
	 */
	for(i = 0; i < ndims; i++)
		nelmts *= dimlens[i];
	vlen_data = (hvl_t *)malloc(nelmts * sizeof(hvl_t));
	
	H5_ASSERT(memtype_id = H5Tget_native_type(datatype_id, H5T_DIR_DEFAULT));	

	H5_ASSERT(H5Dread(dataset_id, memtype_id, H5S_ALL, H5S_ALL, H5P_DEFAULT, vlen_data));

	/*
	 * write data into nc
	 */
	NC_ASSERT(nc_put_var(pa_ncid, ncvar, (nc_vlen_t *)vlen_data));

	/*
	 * release
	 */
	H5_ASSERT(H5Dvlen_reclaim (memtype_id, space, H5P_DEFAULT, vlen_data));
	free(vlen_data);

	H5_ASSERT(H5Tclose(memtype_id));
	H5_ASSERT(H5Sclose(space));
	return;
}

void serial_write_array(int dataset_id, int pa_ncid, const char *name, int datatype_id, nc_type nctype)
{
	int			space, space_type, ncvar, memtype_id, super_dtype, super_memtype;
	int			dimids[DIM_SIZE];
	int			ndims, array_ndims, sum_ndims, i;
	hsize_t		dimlens[DIM_SIZE], array_dimlens[DIM_SIZE];
	char		dimnames[DIM_SIZE][DIM_NAME_LEN];
	size_t		type_size;
	hsize_t		bufsize;
	
	trans_attr_t	tattr;
	unsigned char	*databuf = NULL;

	/*
	 * get data space info
	 */
	H5_ASSERT(space = H5Dget_space(dataset_id));
	H5_ASSERT(space_type = H5Sget_simple_extent_type(space));

	/* get dimension info */
	H5_ASSERT(ndims = H5Sget_simple_extent_dims(space, dimlens, NULL));
	H5_ASSERT(array_ndims = H5Tget_array_dims2(datatype_id, array_dimlens));
	sum_ndims = ndims + array_ndims;
	ASSERT(sum_ndims <= DIM_SIZE);

	
	for(i = ndims; i < sum_ndims; i++)
		dimlens[i] = array_dimlens[i-ndims];

	H5_ASSERT(super_dtype = H5Tget_super(datatype_id));
	nctype = get_nctype(super_dtype);
	ASSERT(nctype != TO_NC_ARRAY);
	
	/* 
	 * define dimension and variable in nc
	 */
	for(i = 0; i < sum_ndims; i++){
		memset(dimnames[i], 0, DIM_NAME_LEN);
		strcpy(dimnames[i], name);
		strcat(dimnames[i], cords[i]);	/* set names of dim */
		NC_ASSERT(nc_def_dim(pa_ncid, dimnames[i], dimlens[i], &(dimids[i])));
	}
	
	nctype = define_nc_dtype(pa_ncid, name, super_dtype, nctype);
	NC_ASSERT(nc_def_var(pa_ncid, name, nctype, sum_ndims, dimids, &ncvar));

	/*
	 * write attribute
	 */
	tattr.pa_ncid = pa_ncid;
	tattr.varid = ncvar;	
	tattr.type = DATASET;
	tattr.variable = 0;
	H5Aiterate(dataset_id, H5_INDEX_NAME, H5_ITER_NATIVE, NULL, conv_attribute_cb, &tattr);


	/*
	 * NULL space
	 */
	if(space_type == H5S_NULL || space_type == H5S_NO_CLASS){

		my_syslog(LOG_WARNING, "ph5toNC: WARNING--meeting a empty dataset%s\n", name);

		H5_ASSERT(H5Sclose(space));
		return;
	}

	/*
	 * read data from hdf5
	 */

	/* set the hyperslab of file space and memory space for each process */
	
	/* get the super type of array */	
	H5_ASSERT(memtype_id = H5Tget_native_type(datatype_id, H5T_DIR_DEFAULT));	
	H5_ASSERT(super_memtype = H5Tget_native_type(super_dtype, H5T_DIR_DEFAULT));

	/* allocate mem for databuf */
	type_size = MAX(H5Tget_size(super_memtype), H5Tget_size(super_dtype));
	bufsize = type_size;
	
	for(i = 0; i < sum_ndims; i++)
		bufsize *= dimlens[i];

	/* test the if there is data overflow */
	ASSERT(bufsize == (hsize_t)((size_t)bufsize));

	/* test for multiple processes */
	debug_printf("bufsize = %llu\n", bufsize);
	
	databuf = (unsigned char *)malloc((size_t)bufsize);
	ASSERT(databuf);
	memset(databuf, 0, bufsize);
		
	H5_ASSERT(H5Dread(dataset_id, memtype_id, H5S_ALL, H5S_ALL, H5P_DEFAULT, databuf));

	NC_ASSERT(nc_put_var(pa_ncid, ncvar, databuf));
	
	/*
	 * release
	 */
	H5_ASSERT(H5Dvlen_reclaim(memtype_id, space, H5P_DEFAULT, databuf));
	free(databuf);
	
	H5_ASSERT(H5Tclose(super_dtype));
	H5_ASSERT(H5Tclose(super_memtype));
	H5_ASSERT(H5Tclose(memtype_id));
	H5_ASSERT(H5Sclose(space));
	
	return;
}

int check_datatype(int datatype_id, int nctype)
{
	/*
	 * string type
	 */
	if(nctype == NC_STRING)
		return VARIABLE_TYPE;

	/*
	 * vlen type
	 */
	if(nctype == NC_VLEN)		
		return check_vlen(datatype_id);

	/*
	 * compoud type
	 */
	if(nctype == NC_COMPOUND)
		return check_compound(datatype_id);

	/* NC_NAT */
	if(nctype == NC_NAT)
		return UNSUPPORT_TYPE;
	/*
	 * array type
	 */
	if(nctype == TO_NC_ARRAY)
		return check_array(datatype_id);

	return PARALLEL_TYPE;
}

int check_compound(int dtype_id)
{
	ASSERT(dtype_id >= 0);
	
	int memb_dtype, field_type;
	int i, nmembs, ret, status;

	H5_ASSERT(nmembs = H5Tget_nmembers(dtype_id));	

	ret = PARALLEL_TYPE;

	for(i = 0; i < nmembs ; i++){

		/* get member datatype */
		memb_dtype = H5Tget_member_type(dtype_id, i);
		H5_ASSERT(memb_dtype);
		
		field_type = get_nctype(memb_dtype);
	
		status = PARALLEL_TYPE;

		if(field_type == NC_NAT)
			status = UNSUPPORT_TYPE;

		else if(field_type == NC_STRING)
			status = VARIABLE_TYPE;

		else if(field_type == NC_COMPOUND)
			status = check_compound(memb_dtype);

		else if(field_type == NC_VLEN)
			status = check_vlen(memb_dtype);	

		else if(field_type == TO_NC_ARRAY)
			status = check_vlen(memb_dtype);
		
		H5_ASSERT(H5Tclose(memb_dtype));
		
		if(status == UNSUPPORT_TYPE)
		  return status;
		else if(status == VARIABLE_TYPE)
		  ret = status;
	}

	return ret;
}

int check_vlen(int dtype_id)
{
	ASSERT(dtype_id >= 0);
	
	int field_type, memb_type;
	int ret;

	memb_type = H5Tget_super(dtype_id);
	H5_ASSERT(memb_type);
	
	field_type = get_nctype(memb_type);
	
	if(field_type == NC_NAT){
		H5_ASSERT(H5Tclose(memb_type));
		return UNSUPPORT_TYPE;
	}
	
	if(field_type == NC_COMPOUND){
		ret = check_compound(memb_type);
		if(ret == PARALLEL_TYPE)
		  ret = VARIABLE_TYPE;
		H5_ASSERT(H5Tclose(memb_type));
		return ret;
	}

	if(field_type == NC_VLEN){
		H5_ASSERT(H5Tclose(memb_type));
		return check_vlen(memb_type);
	}

	if(field_type == TO_NC_ARRAY){
		ret = check_array(memb_type);
		if(ret == PARALLEL_TYPE)
		  ret = VARIABLE_TYPE;
		H5_ASSERT(H5Tclose(memb_type));
		return ret;	
	}

	H5_ASSERT(H5Tclose(memb_type));
	return VARIABLE_TYPE;
}

int check_array(int dtype_id)
{
	ASSERT(dtype_id >= 0);
	
	int field_type, memb_type;
	int ret = PARALLEL_TYPE;

	H5_ASSERT(memb_type = H5Tget_super(dtype_id));
	field_type = get_nctype(memb_type);
	ASSERT(field_type != TO_NC_ARRAY);
	
	if(field_type == NC_NAT)
		ret = UNSUPPORT_TYPE;
	else if(field_type == NC_COMPOUND)
		ret = check_compound(memb_type);
	else if(field_type == NC_VLEN)
		ret = check_vlen(memb_type);

	H5_ASSERT(H5Tclose(memb_type));
	
	return ret;
}

void format_name(char *str)
{
	replace_chr(str, '/', '_');
	replace_chr(str, '%', '_');
	replace_chr(str, '#', '_');
	replace_chr(str, '\n', '_');
	if(str[0] == ' ')
	  strcpy(str, &str[1]);
	if(strcmp(str, "CLASS") == 0)
	  strcpy(str, "class");
	if(strcmp(str, "NAME") == 0)
	  strcpy(str, "name");
}

void delete_chr(char *newname, char c)
{
	char	tname[VAR_NAME_LEN];
	size_t	len;
	int		i;

	memset(tname, 0, VAR_NAME_LEN);
	len = strlen(newname);

	for(i = 0; i < len; i++){
		if(newname[i] == c){
			if(i > 0)
			  strncpy(tname, newname, i);
			if(i < len-1)
			  strcat(tname, &newname[i+1]);
			i--;
			strncpy(newname, tname, VAR_NAME_LEN);
			memset(tname, 0, VAR_NAME_LEN);
		}
	}
}

void replace_chr(char *s, int old, int new)
{
	ASSERT(s != NULL);
	while(*s)
	{
		if(*s == old)
		  *s = new;
		s++;
	}
}
