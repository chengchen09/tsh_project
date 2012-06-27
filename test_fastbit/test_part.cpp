/***********************************************************************
* Filename : test_part.cpp
* Create : XXX 2011-12-11
* Created Time: 2011-12-11
* Description: 
* Modified   : 
* **********************************************************************/

#include <ibis.h>
#include <iostream>
#include <stdio.h>
#include "cc_part.h"

using namespace std;

int main(int argc, char **argv) {
    
    if (argc < 3) {
	printf("Usage: test_part array-name threshold\n");
	return 1;
    }

    struct timeval start, end;
    
    memset(&start, 0, sizeof(start));
    memset(&end, 0, sizeof(end));

    string arrname(argv[1]);
    string whereClause;
    uint32_t nRows;

    whereClause = arrname + "<=" + argv[2];
    /*if (arrname == "etop")
	nRows = 233312401;
    else if (arrname == "pres")
	nRows = 386929200;
    else {
	printf("illegal array name!\n");
	return 1;
    }*/

    string cmd = "SELECT " + arrname + " FROM " + arrname + " WHERE " + whereClause;
    printf("%s\n", cmd.c_str());

    // time start point
    gettimeofday(&start, NULL);
  
    string path;
    path = "./d" + arrname;
    CCPart apart(path.c_str(), static_cast<const char*>(0));
    ibis::query aquery(ibis::util::userName(), &apart);
    ibis::array_t<float> *arr;
    //apart.set_numRows(nRows);
    int ierr = aquery.setWhereClause(whereClause.c_str());
    if (ierr < 0) {
	std::clog << *argv << " setWhereClause(" << argv[2]
	    << ") failed with error code " << ierr << std::endl;
	return -2;
    }
    aquery.setSelectClause(arrname.c_str());
    arr = aquery.getQualifiedFloats(arrname.c_str());

    gettimeofday(&end, NULL); 

    std::cout<<"size: "<<arr->size()<<endl;
    
    double time = end.tv_sec - start.tv_sec + (double)(end.tv_usec - start.tv_usec) / 1000000;
    /*for (size_t i = 0; i < arr->size(); i++) {
	printf("%f\n", (*arr)[i]);
    }*/
    printf("time used: %f\n", time);

    return 0;
}
