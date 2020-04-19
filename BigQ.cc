#include "BigQ.h"
#include <cstring>
using namespace std;

BigQ::BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen): inPipe(in), outPipe(out), sortorder(sortorder), runLength(runlen)
{
    pthread_create(&sortThread, NULL, phaseOne, (void *) this);
}

void* BigQ::phaseOne(void* arg)
{
    BigQ* args = (BigQ*) arg;
    
    //string tmpSortFileName = "runFile.tmp";
    string tmpSortFileName = args->randomStrGen(10);

    char *C = new char[tmpSortFileName.size()+1];
	std::strcpy(C, tmpSortFileName.c_str());

    args->file.Open(0, C);
    args->file.AddPage(new Page(), -1);

    vector<Record*> allRecsInRun;  // vector of all records in one run
    int pageNumber = 0;
    
    Record record;  // Used to record read from pipe.
    Page page;  // Used to temporarily store records.
    
    while (args->inPipe.Remove(&record)){
        Record* rec = new Record();
        rec->Copy(&record);
        
        if (!page.Append(&record)){
            if (++pageNumber == args->runLength){
                args->sortAndWrite(allRecsInRun);
                
                // Restore default states.
                pageNumber = 0;
            }
            page.EmptyItOut();
            page.Append(&record);
        }
        allRecsInRun.emplace_back(rec);
    }

    // check if nothing was read from the input pipe
    if (allRecsInRun.empty()) {
        args->outPipe.ShutDown();
        args->file.Close();
        remove(tmpSortFileName.c_str());
        pthread_exit(NULL);
    }
    // Sort records in the record vector
    args->sortAndWrite(allRecsInRun);

    args->phaseTwo(args, tmpSortFileName);      // Start of phase 2
    delete[] C;
}

void BigQ::phaseTwo(BigQ* args, string tmpSortFileName){

    vector<Page> tmpPage(args->totalRuns);  // get first page of every run into vector

    auto myComp = [&](SortRec* left, SortRec* right){
        ComparisonEngine cEngine;
        return cEngine.Compare(&(left->rec), &(right->rec), &sortorder) > 0;
    };

    priority_queue<SortRec*, vector<SortRec*>, decltype( myComp )> priorityQueue(myComp);



    for(int i = 0; i < args->totalRuns; i++)
    {
        args->file.GetPage(&tmpPage[i], args->startIndex[i]++);
        SortRec* sortRec = new SortRec();
        sortRec->runNumber = i;
        tmpPage[i].GetFirst(&(sortRec->rec));
        priorityQueue.emplace(sortRec);
    }

    while(!priorityQueue.empty())
    {
        SortRec* sortRec = priorityQueue.top();
        priorityQueue.pop();
        int run_index = sortRec->runNumber;
        args->outPipe.Insert(&(sortRec->rec));

        if(tmpPage[run_index].GetFirst(&(sortRec->rec)))
        {
            priorityQueue.emplace(sortRec);
        }
        else if(args->startIndex[run_index] < args->endIndex[run_index])
        {
            args->file.GetPage(&tmpPage[run_index], args->startIndex[run_index]++);
            tmpPage[run_index].GetFirst(&(sortRec->rec));
            priorityQueue.emplace(sortRec);
        }
        else
        {
            delete sortRec;
        }
    }

    args->outPipe.ShutDown();
    args->file.Close();
    remove(tmpSortFileName.c_str());
    pthread_exit(NULL);

}

void BigQ::sortAndWrite(vector<Record*> &sorting)
{
    sort(sorting.begin(), sorting.end(), [&](Record* left, Record* right){
        ComparisonEngine cEngine;
        return cEngine.Compare(left, right, &sortorder) < 0;
    });



    Page page;
    startIndex.push_back(file.GetLength() - 1);  // store page number of start of run in vector
    for (auto rec : sorting)
    {
        if (!page.Append(rec))
        {
            file.AddPage(&page, file.GetLength() - 1);
            page.EmptyItOut();
            page.Append(rec);
            delete rec;
            rec = nullptr;
        }
    }
    file.AddPage(&page, file.GetLength() - 1);
    endIndex.push_back(file.GetLength() - 1);  // store page number of end of run in vector
    ++totalRuns;
    sorting.clear();
}

string BigQ::randomStrGen(int length)
{
    static string charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    string result;
    result.resize(length);

    for (int i = 0; i < length; i++) {
        result[i] = charset[rand() % charset.length()];
    }

    return result;
}

