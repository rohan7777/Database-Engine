

#ifndef SORTED_H
#define SORTED_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Pipe.h"
#include "BigQ.h"
#include "GenericDBFile.h"
#include "DBFile.h"

#include <iostream>
#include <fstream>
#include <sstream>
#include <string.h>
#include <stdio.h>


class Sorted: public GenericDBFile {
private:

    string DBFilePath;  // file path of DBFile. Used to remove old file and rename tmp file during merging.

    int runLength;  // runlength for BigQ
    OrderMaker sortOrder;  // Sort order for sorted file
    OrderMaker* QueryOrder = nullptr;
    OrderMaker* literalQueryOrder = nullptr;

    Pipe inPipe = Pipe(100);  // Push new added records to BigQ through in pipe.
    Pipe outPipe = Pipe(100);  // Get sorted records from BigQ through out pipe.
    BigQ* bigQ = nullptr;

    void toWriteMode();  // Switch to writing mode
    void toReadMode();  // Switch to reading mode
    void makeSortOrder (OrderMaker* , OrderMaker* , CNF &);                             // function to create ordermaker object from passed cnf
    off_t binarySearch (Record &, Record &, OrderMaker* , OrderMaker* );     // returns the page with record that matches queryOrder

public:
    int Create (const char *fpath, void *startup);
    int Open (const char *fpath);

    void Add (Record &addme);
    int GetNext (Record &fetchme);
    int GetNext (Record &fetchme, CNF &cnf, Record &literal);
};

#endif //SORTED_H
