#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "Heap.h"


// stub file .. replace it with your own DBFile.cc

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    
    if (f_type == heap) {
        myInternalVar = new Heap();
    }
    else if (f_type == sorted) {
        myInternalVar = new Sorted();
    }
    
    return myInternalVar->Create(f_path, startup);
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
    myInternalVar->Load(f_schema, loadpath);
}

int DBFile::Open (const char *f_path) {
    // Firstly open .metadata file and read the first line to make sure whether it's a heap file or sorted file.
    string metaFilePath = f_path;
    metaFilePath += ".metadata";
    ifstream f;
    f.open(metaFilePath);
    
    string DBFileType = "";
    f >> DBFileType;
    
    if (DBFileType == "heap") {
        myInternalVar = new Heap();
    }
    else if (DBFileType == "sorted") {
        myInternalVar = new Sorted();
    }
    f.close();
    
    return myInternalVar->Open(f_path);
}

void DBFile::MoveFirst () {
    myInternalVar->MoveFirst();
}

int DBFile::Close () {
    return myInternalVar->Close();
}

void DBFile::Add (Record &rec) {
    myInternalVar->Add(rec);
}

int DBFile::GetNext (Record &fetchme) {
    return myInternalVar->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    return myInternalVar->GetNext(fetchme, cnf, literal);
}

