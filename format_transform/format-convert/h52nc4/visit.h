/***********************************
 * file:	visit.h
 * author:	Chen Cheng
 * date:	10/08/31
 * version:	1.0
 **********************************/

#ifndef _VISIT_H_
#define _VISIT_H_

void	init_visited();
void	init_unvst();

int		check_visited(int objaddr);
void	set_visited(int objaddr);
void	set_revst();
int		check_revst();

void	rcd_vlds_info(int pa_h5id, int pa_ncid, const char *name, int addr);
int		get_vlds_size();
void	pop_vlds(int *pa_h5id, int *pa_ncid, char *name, int *addr);

int		check_unvst(int addr);

void	init_unvst_addrs();
void	rcd_unvst_addr(int addr);
int		check_unvst_addr(int addr);

char *	get_suffix();
#endif

