#include <stdlib.h>
#include <stdio.h>
#include "mpi.h"
#include "hdf5.h"
#include "netcdf.h"

//#include<stdlib.h>
//#include<string.h>
#include <libxml/xmlreader.h>

//static  int sourcefilename = 0;
//static  int targetfilename = 0;
static  int dimensions = 0;
static  int dimension = 0;
static  int dimensionname = 0;
static  int dimensionlength = 0;
static  int variables = 0;
static  int variable = 0;
static  int variablename = 0;
static  int variabletype = 0;
static  int variablendims = 0;
static  int variabledimid = 0;
static  int variabledimids = 0;
static  int variabledimidsdimid = 0;

//for inputing text datas
#define MAX_VARIABLES 1000
#define MAX_DATAS 10000
#define FILESIZE 104857600	//100M chars
#define ERRCODE 2
#define ERR(e) {printf("Error:%s\n", nc_strerror(e)); exit(ERRCODE);}
short shortData[ MAX_VARIABLES ][ MAX_DATAS ];
int intData[ MAX_VARIABLES ][ MAX_DATAS ];
char charData[ MAX_VARIABLES ][ MAX_DATAS ];
float floatData[ MAX_VARIABLES ][ MAX_DATAS ];
double doubleData[ MAX_VARIABLES ][ MAX_DATAS ];

typedef struct _dimension{
	char name[40];
	char length[10];
	struct _dimension *next;
}dimension1, *dimensionPtr;


typedef struct dimensionQueue{
	dimensionPtr front;//队头
	dimensionPtr rear;//队尾
}dimensionQueue;

typedef struct _dimname{
	char name[40];
	struct dimname *next;
}dimname1, *dimnamePtr;

typedef struct dimnameQueue{
	dimnamePtr front;
	dimnamePtr rear;
}dimnameQueue;

typedef struct variable{
	char name[40];
	char type[10];
	char ndims[10];
	dimnameQueue dimids;	
	struct variable *next;
}variable1, *variablePtr;


typedef struct _variableQueue{
	variablePtr front;
	variablePtr rear;
}variableQueue;

typedef struct _data{
	char source_filename[40];
	char target_filename[40];
	dimensionQueue dimQ;
	variableQueue varQ;
}data1;

data1* dataxsz = NULL;
dimensionPtr dimTemp;
variablePtr varTemp;
dimnamePtr dimnameTemp;
//dimnameQueue dimnameQTemp;


int initQueued(dimensionQueue *Q){
	Q->front = (dimensionPtr)malloc(sizeof(dimension1));
//	Q.rear = (dimensionPtr)malloc(sizeof(dimension));
//	if (!Q.front)
//		return false;		
	Q->front->next = NULL;
	Q->rear = Q->front;
	return 1;
}

int initQueuev(variableQueue *Q){
	Q->front = (variablePtr)malloc(sizeof(variable1));
//	Q.rear = (dimensionPtr)malloc(sizeof(dimension));
//	if (!Q.front)
//		return false;		//存储分配失败
	Q->front->next = NULL;
	Q->rear = Q->front;
	return 1;
}

int initQueuedm(dimnameQueue *Q){
	Q->front = (dimnamePtr)malloc(sizeof(dimname1));
//	if(!Q.front)
//		return false;
	Q->front->next = NULL;
	Q->rear = Q->front;
	return 1;
}



int enQueued(dimensionQueue *Q, dimensionPtr e ){
	if (!e) 
		return 0;
	e->next = NULL;
	Q->rear->next = e;
	Q->rear = e;
	return 1;
}

int enQueuev(variableQueue *Q, variablePtr e ){
	if (!e) 
		return 0;
	//printf("%d %s\n", e, "ssssssssss");
	
	e->next = NULL;
//	printf("te");
	Q->rear->next = e;

	Q->rear = e;
//	Q->rear->next =NULL;
	return 1;
}

int enQueuedm(dimnameQueue *Q, dimnamePtr e){
	if (!e) 
		return 0;
	e->next = NULL;
	Q->rear->next = e;
	Q->rear = e;
	return 1;
}

int lengthofDQ(dimensionQueue Q){
	dimensionPtr e = Q.front;
	int ans = 0;
	while(e->next != NULL){
		ans ++;
		e = e->next;
	}
	return ans;
}

/**
 * processNode:
 * @reader: the xmlReader
 *
 * Dump information about the current node
 */
static void
processNode(xmlTextReaderPtr reader) {
    const xmlChar *name, *value;
    int i;
   /* 
    int counter = 0;
    int counter2 = 0;
    int counter3 = 0;
    int counterndim = 0;
    int nowstate = 0;
    char temp[20];
    */

    
    name = xmlTextReaderConstName(reader);
    if (name == NULL)
	name = BAD_CAST "--";

    value = xmlTextReaderConstValue(reader);
    
    /*if (sourcefilename == 1){
	strcpy(dataxsz->source_filename, value);
	printf("%s %s\n",dataxsz->source_filename, "source_filename");
	sourcefilename++;
    }
    if (targetfilename == 1){
	strcpy(dataxsz->target_filename, value);
	printf("%s %s\n", dataxsz->target_filename, "target_filename");
	targetfilename++;
    }
    if (xmlStrcmp(name, "source-filename") == 0){
	sourcefilename++;
    }
    else if (xmlStrcmp(name, "target-filename") == 0){
	targetfilename++;
    }*/

    if(xmlStrcmp(name, "dimensions") == 0){
	dimensions++;
    }
    else if(xmlStrcmp(name, "variables") == 0)
	variables++;


    if (dimensions == 1){
	if (dimension % 2 == 1){
		if (dimensionname % 2 == 1){
			//printf("%s %s %s\n", name, value,"dimensionname");
			if (xmlStrcmp("#text", name) == 0){
				strcpy(dimTemp->name, value);
			//	printf("%s %s %s\n", dimTemp->name, value, "dimensionname");
			}
		}else if(dimensionlength % 2 == 1){
			if (xmlStrcmp("#text", name) == 0){
				strcpy(dimTemp->length, value);
			//	printf("%s %s %s\n", dimTemp->length, value, "dimensionlength");
			}
		}
		if (xmlStrcmp(name, "name") == 0){
			dimensionname ++;
		}else if (xmlStrcmp(name, "length") == 0){
			dimensionlength ++;
		}
		
	}
    }
    //new and add		
    if (dimensions == 1){
	if (xmlStrcmp(name, "dimension") == 0){
		//counter2++;
		dimension++;
		if (dimension %2 == 1){
			dimTemp = (dimensionPtr)malloc(sizeof(dimension1));					       
		}else if(dimension % 2 == 0) {
			enQueued(&dataxsz->dimQ, dimTemp);
		}
			
	}
	
    }
	

    if (variables == 1){
	if (variable % 2 == 1){

		if (variablename % 2 == 1){
			//printf("%s %s %s\n", name, value,"dimensionname");
			if (xmlStrcmp("#text", name) == 0){
				strcpy(varTemp->name, value);
			//	printf("%s %s %s\n", dimTemp->name, value, "dimensionname");
			}
		}else if(variabletype % 2 == 1){
			if (xmlStrcmp("#text", name) == 0){
				strcpy(varTemp->type, value);
			//	printf("%s %s %s\n", dimTemp->length, value, "dimensionlength");
			}
		}else if (variablendims % 2 == 1){
			if (xmlStrcmp("#text", name) == 0){
				strcpy(varTemp->ndims, value);
			}
		}else if (variabledimid % 2 == 1){
			if (xmlStrcmp("#text", name) == 0){
				strcpy(dimnameTemp->name, value);
			}
		}else if (variabledimidsdimid % 2 == 1){
			if (xmlStrcmp("#text", name) == 0){
				strcpy(dimnameTemp->name, value);
				//printf("%s %s\n", value, "dimidsdimid");
			}
		}

		if (variabledimids % 2 == 1){
			if (xmlStrcmp(name, "dimid") == 0){
				variabledimidsdimid++;
				if (variabledimidsdimid % 2 == 1){
					dimnameTemp = (dimnamePtr)malloc(sizeof(dimname1));
					//printf("%d %s\n", variabledimidsdimid, "newdinamefor the queue");
				}else
					enQueuedm(&varTemp->dimids, dimnameTemp);
			}
		}else if (variabledimids % 2 == 0){
			if (xmlStrcmp(name, "dimid") == 0){
				variabledimid++;
				if (variabledimid % 2 == 1)
					dimnameTemp = (dimnamePtr)malloc(sizeof(dimname1));
				else 
					enQueuedm(&varTemp->dimids, dimnameTemp);
				
			}
		}

		if (xmlStrcmp(name, "name") == 0){
			variablename ++;
		}else if (xmlStrcmp(name, "type") == 0){
			variabletype ++;
		}else if (xmlStrcmp(name, "ndims") == 0){
			variablendims ++;
		}else if (xmlStrcmp(name, "dimids") == 0){
			variabledimids ++;
			if (variabledimids % 2 == 1){
				//printf("wo jia da lan niu\n");
			//	dimnameTemp = (dimnamePtr)malloc(sizeof(dimname1));
			}
		//	else 
			//	enQueuedm(&varTemp->dimids, dimnameTemp);
		}		

	}
    }

    //new and add
    if (variables == 1){
	if (xmlStrcmp(name, "variable") == 0){
		variable++;
		if (variable % 2 == 1){
			varTemp = (variablePtr)malloc(sizeof(variable1));		
			initQueuedm(&varTemp->dimids);
//			printf("here am i\n");
//			printf("%d   ",varTemp);
		}else if(variable % 2 == 0){
//			printf("%d   ",varTemp);
			enQueuev(&dataxsz->varQ, varTemp);
//			printf("%s\n", "xszissb!!!");
		}
	}
    }

/*
    printf("%d %d %s %d %d", 
	    xmlTextReaderDepth(reader),
	    xmlTextReaderNodeType(reader),
	    name,
	    xmlTextReaderIsEmptyElement(reader),
	    xmlTextReaderHasValue(reader));
    printf(" %s\n", value);
*/
  /*  if (value == NULL)
	printf("\n");
    else {
        if (xmlStrlen(value) > 40)
            printf(" %.40s...\n", value);
        else
	    printf(" %s\n", value);
    }
*/
}

/**
 * streamFile:
 * @filename: the file name to parse
 *
 * Parse and print information about an XML file.
 */
static void
streamFile(const char *filename) {
    xmlTextReaderPtr reader;
    int ret;

    reader = xmlReaderForFile(filename, NULL, 0);
    if (reader != NULL) {
        ret = xmlTextReaderRead(reader);
        while (ret == 1) {
            processNode(reader);
            ret = xmlTextReaderRead(reader);
        }
        xmlFreeTextReader(reader);
        if (ret != 0) {
            fprintf(stderr, "%s : failed to parse\n", filename);
        }
    } else {
        fprintf(stderr, "Unable to open %s\n", filename);
    }
}

int main(int argc, char **argv) {
	if(argc < 4){
		printf("Usage: text2nc4 xml-infile text-infile nc-outfile\n");
		return 1;
	}

	//MPI_Init( 0, 0 );
	//MPI_File mfile;
	//MPI_Status status;

	/* init */
	int numOfDim = 0;
	int numOfVar = 0;
	int numofDimForVar = 0;

	/* MPI stuff */
	int mpi_namelen;
	char mpi_name[MPI_MAX_PROCESSOR_NAME];
	MPI_Comm comm = MPI_COMM_WORLD;
	MPI_Info info = MPI_INFO_NULL;
	
	/* MPI init */
	int rank, size;
	MPI_Init( 0, 0 );
	MPI_Comm_rank(MPI_COMM_WORLD, &rank) ;
	MPI_Comm_size(MPI_COMM_WORLD, &size) ;
        MPI_Get_processor_name(mpi_name, &mpi_namelen);
	MPI_File mfile;
	MPI_Status status;
	

    /*
     * this initialize the library and check potential ABI mismatches
     * between the version it was compiled for and the actual shared
     * library used.
     */
//    LIBXML_TEST_VERSION

    dataxsz = (data1*)malloc(sizeof(data1));
    initQueued(&dataxsz->dimQ);
    initQueuev(&dataxsz->varQ);
//    dataxsz->dimQ.front = (dimension1*)malloc(sizeof(dimension1));

    streamFile(argv[1]);

    //printf("%s\n", "near the end");

/*analysis text data*/
	/*pre-process*/
	char* buf;
	buf = (char*)malloc( FILESIZE * sizeof( char ) );
	MPI_File_open( MPI_COMM_WORLD, argv[ 2 ], MPI_MODE_RDONLY, MPI_INFO_NULL, &mfile );
	if ( mfile == NULL )
	{
		printf( "no such text file.\n" );
		return 1;
	}
	MPI_File_seek( mfile, 0, MPI_SEEK_SET );
	MPI_File_read( mfile, buf, FILESIZE, MPI_CHAR, &status );
	/*the kernel*/
	int file_count = status.count;
	variablePtr cur_var = dataxsz->varQ.front->next;
	char temp[ 20 ];
	int ivar = 0;
	int ibuf = 0;
	int scount = 0;
	int icount = 0;
	int ccount = 0;
	int fcount = 0;
	int dcount = 0;
	int line = 0;
	int start = -1, end = 0;
	while ( ibuf < file_count - 1 )
	{
		while ( ( end < file_count - 1 ) && buf[ end ] != '\t' && buf[ end ] != '\n' )
			end++;
		int i = 0;
		if ( cur_var->type[ 0 ] == 'c' )
		{
			if ( end - start > 1 )
			{
				charData[ ccount ][ line ] = buf[ start + 1 ];
				ccount++;
			}
			else
			{
				charData[ ccount ][ line ] = '0';
				ccount++;
			}
		}
		else
		{
			for ( i = 0; i < 20; i++ )
			{
				if ( end - start - 1 > i )
					temp[ i ] = buf[ i + start + 1 ];
				else
				{
					temp[ i ] = 'a';
					break;
				}
			}
			if ( end - start > 1 )
			{
				if ( cur_var->type[ 0 ] == 's' )
				{
					shortData[ scount ][ line ] = (short)atoi( temp );
					scount++;
				}
				else if ( cur_var->type[ 0 ] == 'i' || cur_var->type[ 0 ] == 'b' )
				{
					intData[ icount ][ line ] = atoi( temp );
					icount++;
				}
				else if ( cur_var->type[ 0 ] == 'f' )
				{
					floatData[ fcount ][ line ] = (float)atof( temp );
					fcount++;
				}
				else if ( cur_var->type[ 0 ] == 'd' )
				{
					doubleData[ dcount ][ line ] = atof( temp );
					dcount++;
				}
			}
			else
			{
				if ( cur_var->type[ 0 ] == 's' )
				{
					shortData[ scount ][ line ] = 0;
					scount++;
				}
				else if ( cur_var->type[ 0 ] == 'i' || cur_var->type[ 0 ] == 'b' )
				{
					intData[ scount ][ line ] = 0;
					icount++;
				}
				else if ( cur_var->type[ 0 ] == 'f' )
				{
					floatData[ scount ][ line ] = 0;
					fcount++;
				}
				else if ( cur_var->type[ 0 ] == 'd' )
				{
					doubleData[ scount ][ line ] = 0;
					dcount++;
				}
			}
		}
		if ( cur_var->next != NULL )
			cur_var = cur_var->next;
		if ( buf[ end ] == '\n' )
		{
			line++;
			scount = 0;
			icount = 0;
			ccount = 0;
			fcount = 0;
			dcount = 0;
			cur_var = dataxsz->varQ.front->next;
		}
		start = end;
		ibuf = end + 1;
		end++;
	}
	printf( "short: %d\n", shortData[ 0 ][ 0 ] );
	printf( "int: %d\n", intData[ 0 ][ 0 ] );
	printf( "char: %c\n", charData[ 0 ][ 0 ] );
	printf( "float: %f\n", floatData[ 0 ][ 0 ] );
	printf( "double: %f\n", doubleData[ 0 ][ 0 ] );
	/*post-process*/
	MPI_File_close( &mfile );
	free( buf );

	/* end read file (part 2)*/

	/* start part 3 */
	/* define IDs */
	
	int ncid;
	int dimids[MAX_VARIABLES];
	int varids[MAX_VARIABLES];
	size_t* start1;
	size_t* count;

	int retval;
        /* create the file. */
        if ((retval = nc_create_par(argv[3], NC_NETCDF4|NC_MPIIO, comm, info, &ncid)))
                ERR(retval);

	/* define the dimemsions. hand back an id. */
	dimensionPtr my = dataxsz->dimQ.front;
	int count_dim = 0;
	while (my -> next != NULL) {
		my = my -> next;
		if ((retval = nc_def_dim(ncid, my->name, atoi(my->length), &dimids[count_dim])))
                	ERR(retval);
		count_dim ++;
	}
	printf("end define dim\n");
	/* define the varible */
	int count_var = 0;
	int ii = 0;
	int jj = 0;     	//for inner count
	int *ddimids;
	variablePtr myv = dataxsz->varQ.front;
	dimnamePtr mydm;
	while (myv -> next != NULL){
		myv = myv -> next;
		ddimids = (int *)malloc(atoi(myv->ndims));
		mydm = myv->dimids.front;
		for (ii = 0; ii < atoi(myv -> ndims); ii ++){
			mydm = mydm -> next;
			my = dataxsz->dimQ.front;
			jj = 0;
			while (my->next != NULL){
				my = my->next;
				if (strcmp(my->name, mydm->name) == 0){
					ddimids[ii] = dimids[jj];
					break;
				}
				else {
					jj ++;
				}	
			}
		}
		if (strcmp(myv->type, "byte") == 0){
			if ((retval = nc_def_var(ncid, myv->name, NC_BYTE, atoi(myv->ndims), &ddimids[0], &varids[count_var])))
				ERR(retval);
		} else if (strcmp(myv->type, "short") == 0){
			if ((retval = nc_def_var(ncid, myv->name, NC_SHORT, atoi(myv->ndims), &ddimids[0], &varids[count_var])))
				ERR(retval);
		} else if (strcmp(myv->type, "int") == 0) {
			if ((retval = nc_def_var(ncid, myv->name, NC_INT, atoi(myv->ndims), &ddimids[0], &varids[count_var])))
				ERR(retval);
		} else if (strcmp(myv->type, "float") == 0){
			if ((retval = nc_def_var(ncid, myv->name, NC_FLOAT, atoi(myv->ndims), &ddimids[0], &varids[count_var])))
				ERR(retval);
		} else if (strcmp(myv->type, "double") == 0){
			if ((retval = nc_def_var(ncid, myv->name, NC_DOUBLE, atoi(myv->ndims), &ddimids[0], &varids[count_var])))
				ERR(retval);
		} else {
			if ((retval = nc_def_var(ncid, myv->name, NC_CHAR, atoi(myv->ndims), &ddimids[0], &varids[count_var])))
				ERR(retval);
		}
		free (ddimids);
		count_var ++;
	}
	printf("end define variable\n");
	/* end define mode */
        if ((retval = nc_enddef(ncid)))
                ERR(retval);
	
	for (ii = 0; ii < count_var; ii ++)
	{
		if ((retval = nc_var_par_access(ncid, varids[ii], NC_INDEPENDENT)))
         	       ERR(retval);
	}
	
	printf("start put in data\n");
	/* write the data in ncfile */
	int num_int = 0;
	int num_char = 0;
	int num_double = 0;
	int num_short = 0;
	int num_float = 0;
	myv = dataxsz->varQ.front;
	//dimnamePtr mydm;
	int iii = 0;
	int start_s = 1;
	for (ii = 0; ii < count_var; ii ++)
	{
		myv = myv -> next;
		start_s = 1;
		start1 = (size_t*)malloc(atoi(myv->ndims));
		count = (size_t*)malloc(atoi(myv->ndims));
		printf("int : %d\n", ii);
		mydm = myv->dimids.front;
		iii = 0;
		mydm = mydm ->next;
		iii ++;
		while (mydm -> next != NULL){
		//	printf("")		
			mydm = mydm->next;
			start1[iii] = 0;
			my = dataxsz->dimQ.front;
			while (my->next != NULL){
				my = my->next;
				if (strcmp(my->name, mydm->name)){
					count[iii] = atoi(my->length);
					break;
				}
			}
			start_s *= count[iii];
			iii ++;
		}
		my = dataxsz->dimQ.front;
		mydm = myv->dimids.front;
		while (my->next != NULL){
			my = my -> next;
			if (strcmp(my->name, mydm->name)){
				break;
			}
		}
		start1[0] = (rank*atoi(my->length))/size;
		count[0] = (rank + 1)*atoi(my->length)/size - rank*atoi(my->length)/size;
		
		start_s *= start1[0];
		printf("variable type : ");
		printf("%s\n", myv->type);	
		if (strcmp(myv->type, "byte") == 0)
		{
			printf("byte\n");
			if ((retval = nc_put_vara_ubyte(ncid, varids[ii], start1, count, (char *)&intData[num_int][start_s])))
                		ERR(retval);
			num_int ++;
		}

		if (strcmp(myv->type, "short") == 0)
		{
			printf("short\n");
			if ((retval = nc_put_vara_short(ncid, varids[ii], start1, count, &shortData[num_short][start_s])))
                		ERR(retval);
			num_short ++;
		}

		if (strcmp(myv->type, "int") == 0)
		{
			printf("int\n");
			if ((retval = nc_put_vara_int(ncid, varids[ii], start1, count, &intData[num_int][start_s])))
                		ERR(retval);
			num_int ++;
		}

		if (strcmp(myv->type, "char") == 0)
		{
			printf("char\n");
			if ((retval = nc_put_vara_text(ncid, varids[ii], start1, count, &charData[num_char][start_s])))
                		ERR(retval);
			num_char ++;
		}

		if (strcmp(myv->type, "float") == 0)
		{
			printf("float\n");
			if ((retval = nc_put_vara_float(ncid, varids[ii], start1, count, &floatData[num_float][start_s])))
                		ERR(retval);
			num_float ++;
		}

		if (strcmp(myv->type, "double") == 0)
		{
			printf("double\n");
			if ((retval = nc_put_vara_double(ncid, varids[ii], start1, count, &doubleData[num_double][start_s])))
                		ERR(retval);
			num_double ++;
		}
		
		free(start1);
		free(count);
	}
	printf("end put data\n");
        /* close the file */
        if ((retval = nc_close(ncid)))
                ERR(retval);

        printf("*** SUCCESS writing file %s!\n", argv[3]);
	
/*
     dimensionPtr my = dataxsz->dimQ.front;
    int xszwyh = 0;
    while (my != NULL){
	xszwyh++;
	printf("%s %s %s\n", my->name, my->length, "just do it");
	my = my->next;
    }
//    printf("%d %s\n", xszwyh, "yes, we can");

    xszwyh = 0;
    variablePtr myv = dataxsz->varQ.front;
    dimnamePtr mydm;
    while (myv != NULL){
	xszwyh ++;
//	printf("%s %s %s %s\n", myv->name, myv->type, myv->ndims, "oh, my love");
	
	mydm = myv->dimids.front;
	while (mydm != NULL){
	//	printf("%s %s\n", mydm->name, "I can play");
		mydm = mydm->next;
	}
	myv = myv->next;
   }

*/
  //  printf("%d %s\n", xszwyh, "yes, we can");
    /*
     * Cleanup function for the XML library.
     */
    xmlCleanupParser();
    /*
     * this is to debug memory for regression tests
     */
    xmlMemoryDump();
    
    //printf("%s %s\n", dataxsz->source_filename, "end");
    MPI_Finalize();
    return(0);
}
/*
#else
int main(void) {
    fprintf(stderr, "XInclude support not compiled in\n");
    exit(1);
*/
