
#ifndef GENERICDBFILE_H
#define GENERICDBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Pipe.h"
#include "BigQ.h"

#include <iostream>
#include <fstream>
#include <sstream>
#include <string.h>
#include <stdio.h>

typedef enum {heap, sorted, tree} fType;
typedef enum {READ, WRITE, QUERY} mType;

class GenericDBFile {

protected:
    File file;  // One instance of File for one instance of DBFile.

    Page writePage;  // Writting buffer, used to store added records less than a page.
    Page readPage;  // Reading buffer, used to store copy of current page.

    off_t pageNumber = -1;  // Pointer of DBFile indicating which page we are visiting.
    ComparisonEngine cEngine;

    mType mode = READ;  // Used to identify current stage. If the program is in reading mode, and it will perform an operation belonging to writing, then the program need to switch to writing mode first.

    virtual void toWriteMode() = 0;  // Switch to writing mode
    virtual void toReadMode() = 0;  // Switch to reading mode

public:
    virtual int Create (const char *fpath, void *startup) = 0;
    virtual int Open (const char *fpath) = 0;
    virtual int Close ();

    virtual void Load (Schema &myschema, const char *loadpath);

    virtual void MoveFirst ();
    virtual void Add (Record &addme) = 0;
    virtual int GetNext (Record &fetchme);
    virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;
};


#endif //GENERICDBFILE_H
