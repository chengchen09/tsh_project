/***********************************************************************
* Filename : cc_part.h
* Create : XXX 2011-12-11
* Created Time: 2011-12-11
* Description: 
* Modified   : 
* **********************************************************************/
#ifndef CC_PART_H
#define CC_PART_H

#include <ibis.h>
#include <string>

using namespace std;

class CCPart: public ibis::part {
    public:
	void set_numRows(uint32_t n) {nEvents = n;}
	void set_columnName(string& name) {
	    /*ibis::column *col = columns[0];
	    for (it = columns.begin(); it != columns.end(); ++ it)
		cols.push_back(new ibis::column::info(*((*it).second)));*/

	}
	CCPart(const char* adir, const char* bdir):ibis::part(adir, bdir) {}	
};

#endif
