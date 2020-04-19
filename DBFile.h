#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Pipe.h"
#include "BigQ.h"
#include "GenericDBFile.h"
#include "Sorted.h"


#include <iostream>
#include <fstream>
#include <sstream>
#include <string.h>
#include <stdio.h>

// Data structure for input parameter startup of Create function.
struct SortInfo {
    OrderMaker* sortOrder;  // Order of sorted DBFile
    int runLength;  // runlength for BigQ
};

class DBFile {
    
private:

    GenericDBFile *myInternalVar;

public:
	int Create (const char *fpath, fType file_type, void *startup);
	int Open (const char *fpath);
	int Close ();

	void Load (Schema &myschema, const char *loadpath);
	void MoveFirst ();

	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);

};
#endif
