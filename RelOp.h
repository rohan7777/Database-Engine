#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"
#include "BigQ.h"

#include <pthread.h>
#include <vector>

class RelationalOp {
public:
    // blocks the caller until the particular relational operator has run to completion
    // it does so by joinging the operation worker thread to the main thread
    virtual void WaitUntilDone () = 0;

    // tell us how much internal memory the operation can use
    virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp {

private:
    pthread_t worker;
    DBFile *inFile;
    Pipe *outPipe;
    CNF *selOp;
    Record *literal;
    int n_pages;

    static void *startOperation(void* arg);

public:
    void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
    void WaitUntilDone ();
    void Use_n_Pages (int n);
};

class SelectPipe : public RelationalOp {
private:
    pthread_t worker;
    Pipe *inPipe, *outPipe;
    CNF *selOp;
    Record *literal;
    int n_pages;

    static void *startOperation(void* arg);

public:
    void Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
    void WaitUntilDone();
    void Use_n_Pages(int n);
};

class Project : public RelationalOp {
private:
    pthread_t worker;
    Pipe *inPipe, *outPipe;
    int *keepMe;
    int numAttsInput, numAttsOutput;
    int n_pages;

    static void *startOperation(void* arg);

public:
    void Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
    void WaitUntilDone();
    void Use_n_Pages(int n);
};

class Join : public RelationalOp {
private:
    pthread_t worker;
    Pipe *inPipeL, *inPipeR, *outPipe;
    CNF *selOp;
    Record *literal;
    int n_pages;

    static void *startOperation(void* arg);
    static void blockJoin(Join* join);
    static void sortMergeJoin(Join* join, OrderMaker &leftOrder, OrderMaker &rightOrder);
    void vectorCleanup(vector<Record*>& v);

public:
    void Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
    void WaitUntilDone();
    void Use_n_Pages(int n);
};

class DuplicateRemoval : public RelationalOp {
private:
    pthread_t worker;
    Pipe *inPipe, *outPipe;
    Schema *mySchema;
    int n_pages;

    static void *startOperation(void* arg);

public:
    void Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
    void WaitUntilDone();
    void Use_n_Pages(int n);
};

class Sum : public RelationalOp {
private:
    pthread_t worker;
    Pipe *inPipe, *outPipe;
    Function *computeMe;
    int n_pages;

    static void *startOperation(void* arg);

public:
    void Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe);
    void WaitUntilDone();
    void Use_n_Pages(int n);
};

class GroupBy : public RelationalOp {
private:
    pthread_t worker;
    Pipe *inPipe, *outPipe;
    OrderMaker *groupAtts;
    Function *computeMe;
    int n_pages;

    static void *startOperation(void* arg);

public:
    void Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
    void WaitUntilDone();
    void Use_n_Pages(int n);
};

class WriteOut : public RelationalOp {
private:
    pthread_t worker;
    Pipe *inPipe;
    FILE *outFile;
    Schema *mySchema;
    int n_pages;

    static void *startOperation(void* arg);
    void writeOut(Record &rec);

public:
    void Run(Pipe &inPipe, FILE *outFile, Schema &mySchema);
    void WaitUntilDone();
    void Use_n_Pages(int n);
};
#endif
