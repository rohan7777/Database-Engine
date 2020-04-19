

#include "Heap.h"
#include <cstring>

int Heap::Create (const char *f_path, void *startup) {
    string metaFilePath = f_path;
    metaFilePath += ".metadata";
    ofstream f(metaFilePath);
    if (!f) return 0;

    f << "heap" << endl;
    f.close();

    // Create DBFile



    file.Open(0, (char*)f_path);
    readPage.EmptyItOut();
    writePage.EmptyItOut();
    file.AddPage(&writePage, -1);
    MoveFirst();

    return 1;
}

int Heap::Open (const char *fpath) {
    file.Open(1, (char*)fpath);  // Open existing DBFile
    readPage.EmptyItOut();
    writePage.EmptyItOut();
    MoveFirst();
    return 1;
}

void Heap::Add (Record &addme) {
    toWriteMode();
    // Append record to the end of writing buffer. In case of full, write records in
    // the writing buffer to File and append record after cleaned.
    if (!writePage.Append(&addme)) {
        file.AddPage(&writePage, file.GetLength() - 1);
        writePage.EmptyItOut();
        writePage.Append(&addme);
    }
}

int Heap::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    int recordFound = 0;

    while(GenericDBFile::GetNext(fetchme)) {
        if(cEngine.Compare(&fetchme, &literal, &cnf)){
            recordFound = 1;
            break;
        }
    }

    return recordFound;
}

void Heap::toReadMode() {
    if (mode == READ) return;
    mode = READ;
    file.AddPage(&writePage, file.GetLength() - 1);
    readPage.EmptyItOut();
    writePage.EmptyItOut();
}

void Heap::toWriteMode() {
    if (mode == WRITE) return;
    mode = WRITE;
}