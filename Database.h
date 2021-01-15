
#ifndef Database_h
#define Database_h

#include <iostream>
#include <float.h>

#include "ParseTree.h"
#include "Statistics.h"
#include "Schema.h"
#include "OpTreeNode.h"
#include "string.h"

class Database {

private:
    const string stats_dir = "database/Statistics";
    const string dbMetadata = "database/Database";
    const string catalog_dir = "database/catalog";
    const string db_dir = "database/";
    const string output_dir = "output/";
    const string tpch_dir = "/home/rohan/DBI/Tables/1_GB_tbl/";

    int tableCount = 0;
    unordered_map<string, int> tableMap;
    unordered_map<string, Schema*> schemaMap;
    int outputMode = 1; // 0 for NONE, 1 for STDOUT, 2 for 'myfile'
    Statistics statistics;

    void makePermutationsHelper(vector<string> &foundTables, int index, vector<vector<string>> &resultVec, vector<string> &tempRes);
    vector<vector<string>> getPermutations(vector<string> &seenTable);
    void traverse(OpTreeNode *currNode, int outputMode);
    void copySchema(unordered_map<string, Schema*> &aliasSchemas, char* oldName, char* newName);
    void executeSelect();
    OpTreeNode * buildOperationTree(unordered_map<string, Schema*> &, unordered_map<string, string> &, vector<string> &, int  );

public:
    Database() {};
    ~Database() {};
    void Create();
    void Open();
    void Execute();
    void Close();
    void showTables();

    // for test
    unordered_map<string, int> getTablesinDB(){ return  tableMap;}
    unordered_map<string, Schema*> getSchemaInDB() {return schemaMap;}
};

#endif /* Database_h */
