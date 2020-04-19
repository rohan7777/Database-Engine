

#ifndef HEAP_H
#define HEAP_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Pipe.h"
#include "BigQ.h"
#include "GenericDBFile.h"

#include <iostream>
#include <fstream>
#include <sstream>
#include <string.h>
#include <stdio.h>


class Heap: public GenericDBFile {

private:

    void toWriteMode();  // Switch to writing mode
    void toReadMode();  // Switch to reading mode

public:
    int Create (const char *fpath, void *startup);
    int Open (const char *fpath);

    void Add (Record &addme);
    int GetNext (Record &fetchme, CNF &cnf, Record &literal);
};

#endif //HEAP_H
