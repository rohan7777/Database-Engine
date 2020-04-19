#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Comparison.h"

#include <vector>
#include <queue>
#include <algorithm>

class BigQ
{
public:
	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
    static void* phaseOne(void* arg);
    
private:
    Pipe& inPipe;
    Pipe& outPipe;
    OrderMaker& sortorder;
    int runLength;
    
    File file;  // Used to store sorted run.
    int totalRuns = 0;  // Number of runs.
    
    vector<off_t> startIndex;  // Start offset of each run.
    vector<off_t> endIndex;  // End offset of each run.
    
    pthread_t sortThread;
    
    void sortAndWrite(std::vector<Record*>& sorting);  // Used for internal sorting and storing run to file
    void phaseTwo(BigQ* args, string tmpSortFileName);

    // Structure to hold Priority Q node
    struct SortRec
    {
        Record rec;
        int runNumber;
    };

    string randomStrGen(int length);        // method to generate random name of a temp file
};


#endif
