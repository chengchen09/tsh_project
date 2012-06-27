/***********************************
 * file:	visit.c
 * author:	Chen Cheng
 * date:	10/08/31
 * version:	1.0
 **********************************/

#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "visit.h"

#define MAP_SIZE	10000
#define NAME_LEN	1024
#define	ADDRS_SIZE	10000
#define SUFFIX_SIZE 20

struct id_map_class_t{
	int	id_map[MAP_SIZE];	/* record the obj address visited */
	int	size;				/* the num of the address visited */
	int	revst;				/* revst != 0: there is vl type exiting in hdf5 */	
};

struct obj_t{
	int		addr;				/* address in h5file */
	char	name[NAME_LEN];		/* name in h5file */
	int		visited;			/* if this object has been visited, visited = 1 */
	int		pa_h5id;			/* the parent id in hdf5 */
	int		pa_ncid;			/* the parent id in netcdf */
	struct  obj_t	*next;
};

struct table_t{
	int		nobjs;		/* number of the objs */
	struct obj_t	*objs;		/* list of the objs */
};

struct addrs_t{
	int addrs[ADDRS_SIZE];
	int size;
};

static struct id_map_class_t id_map_class;
static struct table_t	unvst_objs;
static struct addrs_t	unvst_addrs;

static char suffix[SUFFIX_SIZE][5] = {
	"_0", "_1", "_2", "_3", "_4", "_5", "_6", "_7", "_8", "_9",
	"_10", "_11", "_12", "_13", "_14", "_15", "_16", "_17", "_18", "_19"
};
static int suffix_count = -1;

void init_visited()
{
	int i;

	/*
	 * initial id_map_class
	 */
	id_map_class.size = 0;
	id_map_class.revst = 0;
	for(i = 0; i < MAP_SIZE; i++)
	  id_map_class.id_map[i] = -1;
	
}

void init_unvst()
{
	/*
	 * initial unvisited objects table
	 */
	unvst_objs.nobjs = 0;
	unvst_objs.objs = NULL;

}

int check_visited(int addr)
{
	int i;
	for(i = 0; i < id_map_class.size; i++)
	  if(addr == id_map_class.id_map[i]){
		  return 1;
	  }
	
	return 0;
}

void set_visited(int addr)
{
	id_map_class.id_map[id_map_class.size] = addr;
	id_map_class.size++;
}

void set_revst(int addr)
{
	id_map_class.revst = 1;
}

int check_revst()
{
	return id_map_class.revst;
}

void rcd_vlds_info(int pa_h5id, int pa_ncid, const char *name, int addr)
{
	struct obj_t	*newobj;

	newobj			 = (struct obj_t *)malloc(sizeof(struct obj_t));
	newobj->pa_h5id	 = pa_h5id;
	newobj->pa_ncid	 = pa_ncid;
	strcpy(newobj->name, name);
	newobj->addr	 = addr;
	newobj->visited	 = 0;
	newobj->next	 = NULL;

	unvst_objs.nobjs++;
	newobj->next = unvst_objs.objs;
	unvst_objs.objs = newobj;
}

int get_vlds_size()
{
	return unvst_objs.nobjs;
}

void pop_vlds(int *pa_h5id, int *pa_ncid, char *name, int *addr)
{
	struct obj_t *tobj;
	if(unvst_objs.nobjs > 0){
			
		if(pa_h5id) *pa_h5id	= unvst_objs.objs->pa_h5id;
		if(pa_ncid) *pa_ncid	= unvst_objs.objs->pa_ncid;
		if(name)	strcpy(name, unvst_objs.objs->name);
		if(addr)	*addr		= unvst_objs.objs->addr;

		/* free memory */
		unvst_objs.nobjs--;
		tobj = unvst_objs.objs;
		unvst_objs.objs = unvst_objs.objs->next;
		free(tobj);
	}
	else{
		if(pa_h5id) *pa_h5id	= -1;
		if(pa_ncid) *pa_ncid	= -1;
		if(addr)	*addr		= -1;
	}
	return;
}

int check_unvst(int addr)
{
	int i;
	struct obj_t *tobj = unvst_objs.objs;

	for(i = 0; i < unvst_objs.nobjs; i++){
		assert(tobj);
		if(addr == tobj->addr){
			return 1;
		}
		tobj = tobj->next;
	}

	return 0;
}

/*
 * functions for unvst_addrs
 */
void init_unvst_addrs()
{
	unvst_addrs.size = 0;
	memset(unvst_addrs.addrs, -1, sizeof(int) * ADDRS_SIZE);
}

void rcd_unvst_addr(int addr)
{
	unvst_addrs.addrs[unvst_addrs.size] = addr;
	unvst_addrs.size++;
}

int check_unvst_addr(int addr)
{
	int i;

	for(i = 0; i < unvst_addrs.size; i++){
		if(addr == unvst_addrs.addrs[i])
		  return 1;
	}

	return 0;
}

char * get_suffix()
{
	suffix_count++;
	suffix_count = suffix_count % SUFFIX_SIZE;
	return suffix[suffix_count];
}
