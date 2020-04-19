//
// Placed here for entry in Make File
//

#include "GenericDBFile.h"

int GenericDBFile::GetNext (Record &fetchme) {
    // Get record from reading buffer. In case of empty, read a new page to buffer.
    // IF you reach end of file, return 0
    while (!readPage.GetFirst(&fetchme)) {
        if (pageNumber == file.GetLength() - 2) {
            return 0;
        }
        else {
            file.GetPage(&readPage, ++pageNumber);
        }
    }
    return 1;
}

int GenericDBFile::Close () {
    toReadMode();
    return file.Close() ? 1 : 0;
}

void GenericDBFile::Load (Schema &myschema, const char *loadpath) {
    FILE *tblFilePath = fopen(loadpath, "r");
    int numRecs = 0;
    if(tblFilePath == NULL){
        cout << "Can not open the file!" << endl;
        exit(1);
    }
    else{
        Record addme = Record();
        while(addme.SuckNextRecord(&myschema, tblFilePath)) {
            Add(addme);
            ++numRecs;
        }

        fclose(tblFilePath);
        cout << "Loaded " << numRecs << " records" << endl;
    }
}

void GenericDBFile::MoveFirst () {
    toReadMode();
    readPage.EmptyItOut();
    pageNumber = -1;  // Move pointer to the first page.
}