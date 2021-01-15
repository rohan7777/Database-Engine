#ifndef NODE_H
#define NODE_H

#include <iostream>
#include <string>
#include <vector>

#include <stdio.h>
#include "ParseTree.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Schema.h"
#include "Function.h"
#include "Pipe.h"
#include "RelOp.h"

enum NodeType {BASE, SELECTFILE, SELECTPIPE, PROJECT, JOIN, GROUPBY, DUPLICATEREMOVAL, SUM};

static int PIPEID = 0;

class OpTreeNode {
public:
    int pipeID = PIPEID++;
    NodeType myType = BASE;
    Schema *schema = nullptr;

public:
    OpTreeNode *left = nullptr, *right = nullptr;
    Pipe outpipe = Pipe(50);

    virtual void run() = 0;
    virtual void print() = 0;

    int getPipeID() { return pipeID; };
    Schema* getSchema() { return schema; };
    NodeType getType() { return myType; }
};

class SelectFileNode : public OpTreeNode {
private:
    string dbfilePath;
    DBFile dbfile;
    CNF cnf;
    Record literal;
    SelectFile selectFile;

public:
    SelectFileNode(AndList *selectList, Schema *schema, string relName);
    void run();
    void print();
};

class SelectPipeNode : public OpTreeNode {
private:
    CNF cnf;
    Record literal;
    SelectPipe selectPipe;

public:
    SelectPipeNode(OpTreeNode *child, AndList *selectList);
    void run();
    void print();
};

class ProjectNode : public OpTreeNode {
private:
    int *attributesToSelect;
    NameList *attsLeft;
    Project project;

public:
    ProjectNode(OpTreeNode *child, NameList* attrsLeft);
    void run();
    void print();
};

class JoinNode : public OpTreeNode {
private:
    CNF cnf;
    Record literal;
    Join join;
    //  int offset;
    void joinSchema();

public:
    JoinNode(OpTreeNode *leftChild, OpTreeNode *rightChild, AndList *joinList);
    void run();
    void print();
};

class DuplicateRemovalNode : public OpTreeNode {
private:
    DuplicateRemoval duplicateRemoval;

public:
    DuplicateRemovalNode(OpTreeNode *child);
    void run();
    void print();
};

class SumNode : public OpTreeNode {
private:
    FuncOperator *sumFunc;
    Function computeMe;
    Sum sum;
    void sumSchema();

public:
    SumNode(OpTreeNode *child, FuncOperator *func);
    void run();
    void print();
    string joinFunc(FuncOperator *func);
};


class GroupByNode : public OpTreeNode {
private:
    FuncOperator *groupByFunc;
    OrderMaker groupOrder;
    Function computeMe;
    GroupBy groupBy;

    void getOrder(NameList* groupingAtts);
    void groupBySchema();
    string joinFunc(FuncOperator *func);

public:
    GroupByNode(OpTreeNode *child, NameList *groupingAtts, FuncOperator *func);
    void run();
    void print();
};

#endif