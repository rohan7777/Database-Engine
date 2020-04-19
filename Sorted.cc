

#include "Sorted.h"

int Sorted::Create (const char *f_path, void *startup) {
    // The first line of .metadata file is type of DBFile, which is "sorted" here.
    // From the second line, we write information of OrderMaker, each line is a tuple combined with whichAtts and whichTypes and they are separated by ' '.
    DBFilePath = f_path;
    string metaFilePath  = DBFilePath + ".metadata";
    ofstream f(metaFilePath.c_str());
    if (!f) return 0;

    f << "sorted" << endl;
    sortOrder = *((SortInfo*)startup)->sortOrder;
    for (int i = 0; i < sortOrder.numAtts; ++i) {
        f << sortOrder.whichAtts[i] << " " << sortOrder.whichTypes[i] << endl;
    }
    f.close();

    // Create DBFile
    file.Open(0, (char*)f_path);
    readPage.EmptyItOut();
    writePage.EmptyItOut();
    file.AddPage(&writePage, -1);  // First page of every File is empty accord to comments of File class.
    return 1;
}


int Sorted::Open (const char *f_path) {
    // Just like different direction of create, we read from .metadata and retrieve information of OrderMaker and reconstruct it.
    DBFilePath = f_path;
    string metaFilePath = DBFilePath + ".metadata";
    ifstream f(metaFilePath.c_str());
    if (!f) return 0;

    string fileLine = "";
    sortOrder.numAtts = 0;
    getline(f, fileLine);
    while (f >> sortOrder.whichAtts[sortOrder.numAtts] && !f.eof()) {
        int type = 0;
        f >> type;
        if (type == 0) sortOrder.whichTypes[sortOrder.numAtts] = Int;
        else if (type == 1) sortOrder.whichTypes[sortOrder.numAtts] = Double;
        else sortOrder.whichTypes[sortOrder.numAtts] = String;
        ++sortOrder.numAtts;
    }
    f.close();
    file.Open(1,(char*) f_path);
    writePage.EmptyItOut();
    readPage.EmptyItOut();
    return 1;
}

void Sorted::Add (Record &rec) {
    toWriteMode();
    inPipe.Insert(&rec);
}

int Sorted::GetNext (Record &fetchme) {
    if (mode == WRITE) {
        toReadMode();
    }

    return GenericDBFile::GetNext(fetchme);
}

int Sorted::GetNext (Record &fetchme, CNF &cnf, Record &literal) {

    // create a ordermaker object by the passed cnf search for the record that passes the cnf
    if (mode != QUERY) {
        toReadMode();
        mode = QUERY;

        // Construct query OrderMaker
        delete QueryOrder;
        delete literalQueryOrder;
        QueryOrder = new OrderMaker();
        literalQueryOrder = new OrderMaker();

        makeSortOrder(QueryOrder, literalQueryOrder, cnf);
        
        // if file is empty return 0
        if (file.GetLength() == 1) return 0;
        else if (QueryOrder->numAtts == 0) {
            //if attributes in cnf does not match the sort order, start linear search from first page
            pageNumber = 0;
            file.GetPage(&readPage, 0);
        }
        else {
            // Binary search for first page that matches the query
            pageNumber = binarySearch(fetchme, literal, QueryOrder, literalQueryOrder);


            // Get the first record that matches the query
            file.GetPage(&readPage, pageNumber);
            while (GetNext(fetchme)) {
                if (cEngine.Compare(&fetchme, QueryOrder, &literal, literalQueryOrder) == 0) {
                    if(cEngine.Compare(&fetchme, &literal, &cnf)){
                        return 1;
                    }
                }
                else if (cEngine.Compare(&fetchme, QueryOrder, &literal, literalQueryOrder) > 0) {
                    return 0;
                }
            }
        }
    }


    // When the file is already in query mode the current pointer is pointing to a record that matches the query
    // so just get the next record
    int recordFound = 0;
    while(GetNext(fetchme)) {
        if (QueryOrder->numAtts > 0 && cEngine.Compare(&fetchme, QueryOrder, &literal, literalQueryOrder) != 0) {
            break;
        }
        if(cEngine.Compare(&fetchme, &literal, &cnf)){
            recordFound = 1;
            break;
        }
    }

    return recordFound;
}

off_t Sorted::binarySearch (Record &fetchme, Record &literal, OrderMaker* queryOrder, OrderMaker* literalQueryOrder){
    off_t left = 0, right = file.GetLength() - 2;
    while (left < right - 1) {
        pageNumber = left + (right - left) / 2;
        file.GetPage(&readPage, pageNumber);
        GetNext(fetchme);
        if (cEngine.Compare(&fetchme, queryOrder, &literal, literalQueryOrder) == 0) {
            right = pageNumber;
        }
        else if (cEngine.Compare(&fetchme, queryOrder, &literal, literalQueryOrder) > 0) {
            right = pageNumber - 1;
        }
        else {
            left = pageNumber;
        }
    }
    return left;
}

void Sorted::makeSortOrder (OrderMaker* queryOrder, OrderMaker* literalQueryOrder, CNF &cnf){

    for (int i = 0; i < sortOrder.numAtts; ++i) {
        bool found = false;
        int literalIndex = 0;
        for (int j = 0; j < cnf.numAnds && !found; ++j) {
            for (int k = 0; k < cnf.orLens[j] && !found; ++k) {
                if (cnf.orList[j][k].operand1 == Literal) {
                    if (cnf.orList[j][k].operand2 == Left && sortOrder.whichAtts[i] == cnf.orList[j][k].whichAtt2 && cnf.orList[j][k].op == Equals) {
                        found = true;
                        queryOrder->whichAtts[queryOrder->numAtts] = sortOrder.whichAtts[i];
                        queryOrder->whichTypes[queryOrder->numAtts] = sortOrder.whichTypes[i];
                        ++queryOrder->numAtts;
                        literalQueryOrder->whichAtts[literalQueryOrder->numAtts] = literalIndex;
                        literalQueryOrder->whichTypes[literalQueryOrder->numAtts] = cnf.orList[j][k].attType;
                        ++literalQueryOrder->numAtts;
                    }
                    ++literalIndex;
                }
                else if (cnf.orList[j][k].operand2 == Literal) {
                    if (cnf.orList[j][k].operand1 == Left && sortOrder.whichAtts[i] == cnf.orList[j][k].whichAtt1 && cnf.orList[j][k].op == Equals) {
                        found = true;
                        queryOrder->whichAtts[queryOrder->numAtts] = sortOrder.whichAtts[i];
                        queryOrder->whichTypes[queryOrder->numAtts] = sortOrder.whichTypes[i];
                        ++queryOrder->numAtts;
                        literalQueryOrder->whichAtts[literalQueryOrder->numAtts] = literalIndex;
                        literalQueryOrder->whichTypes[literalQueryOrder->numAtts] = cnf.orList[j][k].attType;
                        ++literalQueryOrder->numAtts;
                    }
                    ++literalIndex;
                }
            }
        }
        if (!found) break;
    }
}


void Sorted::toWriteMode() {
    if (mode == WRITE) return;
    mode = WRITE;
    bigQ = new BigQ(inPipe, outPipe, sortOrder, runLength);
}

void Sorted::toReadMode() {
    if (mode == READ) return;
    if (mode == QUERY) {
        mode = READ;
        return;
    }
    bool isPipeEmpty = false; // Flag to check which queue gets empty
    bool isFileEmpty = false; 
    
    mode = READ;
    MoveFirst();
    
    inPipe.ShutDown();          // Shutdown the in pipe so that no more records are added into the pipe.
    File tmpFile;
    tmpFile.Open(0, "tempFile");
    
    writePage.EmptyItOut();
    tmpFile.AddPage(&writePage, -1);

   

    Record left;
    if (!outPipe.Remove(&left)) isPipeEmpty = true;

    Record right;
    if (!GetNext(right)) isFileEmpty = true;

    // Run two way merge while both the queue have records
    while (!isPipeEmpty && !isFileEmpty) {
        if (cEngine.Compare(&left, &right, &sortOrder) <= 0) {
            // Write smaller record to writing buffer. If writing buffer if full, add this page to file and start to write to a new empty page.
            if (!writePage.Append(&left)) {
                tmpFile.AddPage(&writePage, tmpFile.GetLength() - 1);
                writePage.EmptyItOut();
                writePage.Append(&left);
            }
            // If one queue is empty, end merging.
            if (!outPipe.Remove(&left)) isPipeEmpty = true;
        }
        else {
            if (!writePage.Append(&right)) {
                tmpFile.AddPage(&writePage, tmpFile.GetLength() - 1);
                writePage.EmptyItOut();
                writePage.Append(&right);
            }
            if (!GetNext(right)) isFileEmpty = true;
        }
    }
    // Determine which queue runs out and append records of the other queue to the end or new file.
    if (isPipeEmpty && !isFileEmpty) {
        if (!writePage.Append(&right)) {
            tmpFile.AddPage(&writePage, tmpFile.GetLength() - 1);
            writePage.EmptyItOut();
            writePage.Append(&right);
        }
        while (GetNext(right)) {
            if (!writePage.Append(&right)) {
                tmpFile.AddPage(&writePage, tmpFile.GetLength() - 1);
                writePage.EmptyItOut();
                writePage.Append(&right);
            }
        }
    }
    else if (!isPipeEmpty && isFileEmpty) {
        if (!writePage.Append(&left)) {
            tmpFile.AddPage(&writePage, tmpFile.GetLength() - 1);
            writePage.EmptyItOut();
            writePage.Append(&left);
        }
        while (outPipe.Remove(&left)) {
            if (!writePage.Append(&left)) {
                tmpFile.AddPage(&writePage, tmpFile.GetLength() - 1);
                writePage.EmptyItOut();
                writePage.Append(&left);
            }
        }
    }
    // If and only if not both queues are empty, append the last probably not full page to the end of file. Because if two queues are empty, there is no need to append an empty page to the file.
    if (!isPipeEmpty || !isFileEmpty) tmpFile.AddPage(&writePage, tmpFile.GetLength() - 1);

    tmpFile.Close();
    remove(DBFilePath.c_str()); // Delete old DBFile
    rename("tempFile", DBFilePath.c_str());  // Rename tmp file to new DBFile with the same name as the old one.

    delete bigQ;
}
