#include "RelOp.h"

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
    this->inFile = &inFile;
    this->outPipe = &outPipe;
    this->selOp = &selOp;
    this->literal = &literal;

    pthread_create(&worker, NULL, startOperation, (void*)this);
}

void *SelectFile::startOperation(void* arg) {
    SelectFile* selectFile = (SelectFile*)arg;
    Record tmpRec;
    ComparisonEngine comEng;

    while (selectFile->inFile->GetNext(tmpRec)) {
        if (comEng.Compare(&tmpRec, selectFile->literal, selectFile->selOp)) {
            selectFile->outPipe->Insert(&tmpRec);
        }
    }
    selectFile->outPipe->ShutDown();
    selectFile->inFile->Close();
    pthread_exit(NULL);
}

void SelectFile::WaitUntilDone () {
    pthread_join (worker, NULL);
}

void SelectFile::Use_n_Pages(int n) {
    this->n_pages = n;
}

void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
    this->inPipe = &inPipe;
    this->outPipe = &outPipe;
    this->selOp = &selOp;
    this->literal = &literal;
    pthread_create(&worker, NULL, startOperation, (void*)this);
}

void *SelectPipe::startOperation(void *arg) {
    SelectPipe* selectPipe = (SelectPipe*)arg;
    Record tmpRec;
    ComparisonEngine comEng;

    while (selectPipe->inPipe->Remove(&tmpRec)) {
        if (comEng.Compare(&tmpRec, selectPipe->literal, selectPipe->selOp)) {
            selectPipe->outPipe->Insert(&tmpRec);
        }
    }
    selectPipe->outPipe->ShutDown();
    pthread_exit(NULL);
}


void SelectPipe::WaitUntilDone() {
    pthread_join(worker, NULL);
}

void SelectPipe::Use_n_Pages(int n) {
    this->n_pages = n;
}

void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
    this->inPipe = &inPipe;
    this->outPipe = &outPipe;
    this->keepMe = keepMe;
    this->numAttsInput = numAttsInput;
    this->numAttsOutput = numAttsOutput;
    pthread_create(&worker, NULL, startOperation, (void*)this);
}

void *Project::startOperation(void *arg) {
    Project* project = (Project*)arg;
    Record tmpRec;

    while (project->inPipe->Remove(&tmpRec)) {
        tmpRec.Project(project->keepMe, project->numAttsOutput, project->numAttsInput);
        project->outPipe->Insert(&tmpRec);
    }

    project->outPipe->ShutDown();
    pthread_exit(NULL);
}

void Project::WaitUntilDone() {
    pthread_join(worker, NULL);
}

void Project::Use_n_Pages(int n) {
    this->n_pages = n;
}

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
    this->inPipeL = &inPipeL;
    this->inPipeR = &inPipeR;
    this->outPipe = &outPipe;
    this->selOp = &selOp;
    this->literal = &literal;
    pthread_create(&worker, NULL, startOperation, this);
}

void *Join::startOperation(void *arg) {

    Join* join = (Join*)arg;

    OrderMaker leftOrder;
    OrderMaker rightOrder;

    if (join->selOp->GetSortOrders(leftOrder, rightOrder)) {
        sortMergeJoin(join, leftOrder, rightOrder);
    }
    else {
        blockJoin(join);
    }

    join->outPipe->ShutDown();
    pthread_exit(NULL);
}

void Join::sortMergeJoin(Join* join, OrderMaker &leftOrder, OrderMaker &rightOrder){
    ComparisonEngine comparisonEngine;

    vector<Record*> leftVector;
    vector<Record*> rightVector;

    Record *tempLeftRec = new Record();
    Record *tempRightRec = new Record();

    int numAttsLeft = 0, numAttsRight = 0, numAttsToKeep = 0;
    int* attsToKeep = nullptr;
    Pipe leftSortPipe(200), rightSortPipe(200);                                             // pipe to get sorted records from BigQ
    BigQ leftBQ = BigQ(*(join->inPipeL), leftSortPipe, leftOrder, join->n_pages);           // BiqQ to sort left records
    BigQ rightBQ = BigQ(*(join->inPipeR), rightSortPipe, rightOrder, join->n_pages);        // BiqQ to sort right records
    bool isLeftEmpty = false, isRightEmpty = false;

    // if pipe is empty, shut down and exit
    if (!leftSortPipe.Remove(tempLeftRec)) {
        join->outPipe->ShutDown();
        cout<< "No records in left relation";
        pthread_exit(NULL);
    }
    else {
        // count the number of attributes in left record
        numAttsLeft = ((int *) tempLeftRec->bits)[1] / sizeof(int) - 1;
    }

    // if the pipe is empty, shut down the pipe and exit
    if (!rightSortPipe.Remove(tempRightRec)) {
        join->outPipe->ShutDown();
        cout<< "No records in right relation";
        pthread_exit(NULL);
    }
    else {
        // count the number of records in the right record
        numAttsRight = ((int *) tempRightRec->bits)[1] / sizeof(int) - 1;
    }

    numAttsToKeep = numAttsLeft + numAttsRight;
    attsToKeep = new int[numAttsToKeep];
    int i = 0;

    // create a new attributes to keep array that stores all the attributes that the joint tuple should have.
    while(i < numAttsLeft){
        attsToKeep[i] = i;
        i++;
    }
    while(i < numAttsToKeep ){
        attsToKeep[i] = i - numAttsLeft;
        i++;
    }

    leftVector.emplace_back(tempLeftRec);
    tempLeftRec = new Record();
    rightVector.emplace_back(tempRightRec);
    tempRightRec = new Record();

    if (!leftSortPipe.Remove(tempLeftRec)) {
        isLeftEmpty = true;
    }
    if (!rightSortPipe.Remove(tempRightRec)) {
        isRightEmpty = true;
    }

    bool isLeftBigger = false, isRightBigger = false;

    while (!isLeftEmpty && !isRightEmpty) {


        if (!isLeftBigger) {
            while (!comparisonEngine.Compare(leftVector.back(), tempLeftRec, &leftOrder)) {
                leftVector.emplace_back(tempLeftRec);
                tempLeftRec = new Record();
                if (!leftSortPipe.Remove(tempLeftRec)) {
                    isLeftEmpty = true;
                    break;
                }
            }
        }

        if (!isRightBigger) {
            while (!comparisonEngine.Compare(rightVector.back(), tempRightRec, &rightOrder)) {
                rightVector.emplace_back(tempRightRec);
                tempRightRec = new Record();
                if (!rightSortPipe.Remove(tempRightRec)) {
                    isRightEmpty = true;
                    break;
                }
            }
        }

        if (comparisonEngine.Compare(leftVector.back(), &leftOrder, rightVector.back(), &rightOrder) > 0) {
            join->vectorCleanup(rightVector);
            isLeftBigger = true;
            isRightBigger = false;
        }
        else if (comparisonEngine.Compare(leftVector.back(), &leftOrder, rightVector.back(), &rightOrder) < 0) {
            join->vectorCleanup(leftVector);
            isLeftBigger = false;
            isRightBigger = true;
        }
        else {
            for (auto recL : leftVector) {
                for (auto recR : rightVector) {
                    if (comparisonEngine.Compare(recL, recR, join->literal, join->selOp)) {
                        Record joinedRec, cpRecR;
                        cpRecR.Copy(recR);
                        joinedRec.MergeRecords(recL, &cpRecR, numAttsLeft, numAttsRight, attsToKeep, numAttsToKeep, numAttsLeft);
                        join->outPipe->Insert(&joinedRec);
                    }
                }
            }

            join->vectorCleanup(leftVector);
            join->vectorCleanup(rightVector);
            isLeftBigger = false;
            isRightBigger = false;
        }

        if (!isLeftEmpty && !isLeftBigger) {
            leftVector.emplace_back(tempLeftRec);
            tempLeftRec = new Record();
            if (!leftSortPipe.Remove(tempLeftRec)) {
                isLeftEmpty = true;
            }
        }
        if (!isRightEmpty && !isRightBigger) {
            rightVector.emplace_back(tempRightRec);
            tempRightRec = new Record();
            if (!rightSortPipe.Remove(tempRightRec)) {
                isRightEmpty = true;
            }
        }
    }

    if (!leftVector.empty() && !rightVector.empty()) {
        while (!isLeftEmpty) {
            if (!isLeftBigger) {
                while (!comparisonEngine.Compare(leftVector.back(), tempLeftRec, &leftOrder)) {
                    leftVector.emplace_back(tempLeftRec);
                    tempLeftRec = new Record();
                    if (!leftSortPipe.Remove(tempLeftRec)) {
                        isLeftEmpty = true;
                        break;
                    }
                }
            }
            if (comparisonEngine.Compare(leftVector.back(), &leftOrder, rightVector.back(), &rightOrder) > 0) {
                join->vectorCleanup(rightVector);
                isLeftBigger = true;
                isRightBigger = false;
                break;
            }
            else if (comparisonEngine.Compare(leftVector.back(), &leftOrder, rightVector.back(), &rightOrder) < 0) {
                join->vectorCleanup(leftVector);
                isLeftBigger = false;
                isRightBigger = true;
            }
            else {
                for (auto recL : leftVector) {
                    for (auto recR : rightVector) {
                        if (comparisonEngine.Compare(recL, recR, join->literal, join->selOp)) {
                            Record joinedRec, cpRecR;
                            cpRecR.Copy(recR);
                            joinedRec.MergeRecords(recL, &cpRecR, numAttsLeft, numAttsRight, attsToKeep, numAttsToKeep, numAttsLeft);
                            join->outPipe->Insert(&joinedRec);
                        }
                    }
                }

                join->vectorCleanup(leftVector);
                join->vectorCleanup(rightVector);
                isLeftBigger = false;
                isRightBigger = false;
                break;
            }

            if (!isLeftEmpty && !isLeftBigger) {
                leftVector.emplace_back(tempLeftRec);
                tempLeftRec = new Record();
                if (!leftSortPipe.Remove(tempLeftRec)) {
                    isLeftEmpty = true;
                }
            }
        }
        while (!isRightEmpty) {
            if (!isRightBigger) {
                while (!comparisonEngine.Compare(rightVector.back(), tempRightRec, &rightOrder)) {
                    rightVector.emplace_back(tempRightRec);
                    tempRightRec = new Record();
                    if (!rightSortPipe.Remove(tempRightRec)) {
                        isRightEmpty = true;
                        break;
                    }
                }
            }
            if (comparisonEngine.Compare(leftVector.back(), &leftOrder, rightVector.back(), &rightOrder) > 0) {
                join->vectorCleanup(rightVector);
                isLeftBigger = true;
                isRightBigger = false;
            }
            else if (comparisonEngine.Compare(leftVector.back(), &leftOrder, rightVector.back(), &rightOrder) < 0) {
                join->vectorCleanup(leftVector);
                isLeftBigger = false;
                isRightBigger = true;
                break;
            }
            else {
                for (auto recL : leftVector) {
                    for (auto recR : rightVector) {
                        if (comparisonEngine.Compare(recL, recR, join->literal, join->selOp)) {
                            Record joinedRec, cpRecR;
                            cpRecR.Copy(recR);
                            joinedRec.MergeRecords(recL, &cpRecR, numAttsLeft, numAttsRight, attsToKeep, numAttsToKeep, numAttsLeft);
                            join->outPipe->Insert(&joinedRec);
                        }
                    }
                }

                join->vectorCleanup(leftVector);
                join->vectorCleanup(rightVector);
                isLeftBigger = false;
                isRightBigger = false;
                break;
            }

            if (!isRightEmpty && !isRightBigger) {
                rightVector.emplace_back(tempRightRec);
                tempRightRec = new Record();
                if (!rightSortPipe.Remove(tempRightRec)) {
                    isRightEmpty = true;
                }
            }
        }
    }

    join->vectorCleanup(leftVector);
    join->vectorCleanup(rightVector);
    delete tempLeftRec;
    tempLeftRec = nullptr;
    delete tempRightRec;
    tempRightRec = nullptr;
}

void Join :: blockJoin(Join* join){

    ComparisonEngine comparisonEngine;

    vector<Record*> leftVector;
    vector<Record*> rightVector;

    Record *tempLeftRec = new Record();
    Record *tempRightRec = new Record();

    int numAttsLeft = 0, numAttsRight = 0, numAttsToKeep = 0;
    int* attsToKeep = nullptr;

    if (!join->inPipeL->Remove(tempLeftRec)) {
        join->outPipe->ShutDown();
        pthread_exit(NULL);
    }
    else {
        numAttsLeft = *((int *) tempLeftRec->bits);
        leftVector.emplace_back(tempLeftRec);
        tempLeftRec = new Record();
    }

    if (!join->inPipeR->Remove(tempRightRec)) {
        join->outPipe->ShutDown();
        pthread_exit(NULL);
    }
    else {
        numAttsRight = *((int *) tempRightRec->bits);
        rightVector.emplace_back(tempRightRec);
        tempRightRec = new Record();
    }

    numAttsToKeep = numAttsLeft + numAttsRight;
    attsToKeep = new int[numAttsToKeep];

    int i = 0;
    while(i < numAttsLeft){
        attsToKeep[i] = i;
        i++;

    }
    while(i < numAttsToKeep ){
        attsToKeep[i] = i - numAttsLeft;
        i++;
    }

    while (join->inPipeL->Remove(tempLeftRec)) {
        leftVector.emplace_back(tempLeftRec);
        tempLeftRec = new Record();
    }
    while (join->inPipeR->Remove(tempRightRec)) {
        rightVector.emplace_back(tempRightRec);
        tempRightRec = new Record();
    }
    for (auto recL : leftVector) {
        for (auto recR : rightVector) {
            if (comparisonEngine.Compare(recL, recR, join->literal, join->selOp)) {
                Record joinedRec;
                joinedRec.MergeRecords(tempLeftRec, tempRightRec, numAttsLeft, numAttsRight, attsToKeep, numAttsToKeep, numAttsLeft);
                join->outPipe->Insert(&joinedRec);
            }
        }
    }
    join->vectorCleanup(leftVector);
    join->vectorCleanup(rightVector);
    delete tempLeftRec;
    tempLeftRec = nullptr;
    delete tempRightRec;
    tempRightRec = nullptr;
}

void Join::WaitUntilDone() {
    pthread_join(worker, NULL);
}

void Join::Use_n_Pages(int n) {
    this->n_pages = n;
}

void Join::vectorCleanup(vector<Record*>& v){
    for (auto r : v) {
        delete r;
        r = nullptr;
    }
    v.clear();
}

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
    this->inPipe = &inPipe;
    this->outPipe = &outPipe;
    this->mySchema = &mySchema;
    pthread_create(&worker, NULL, startOperation, this);
}

void *DuplicateRemoval::startOperation(void *arg) {
    DuplicateRemoval* dr = (DuplicateRemoval*)arg;
    OrderMaker order = OrderMaker(dr->mySchema);
    ComparisonEngine cmpEng;
    Record tmpRec;
    Record nextRec;
    
    Pipe pipe(200);
    BigQ bigQ = BigQ(*(dr->inPipe), pipe, order, dr->n_pages);
    
    
    if (pipe.Remove(&tmpRec)) {
        while (pipe.Remove(&nextRec)) {
            if (cmpEng.Compare(&tmpRec, &nextRec, &order)) {
                dr->outPipe->Insert(&tmpRec);
                tmpRec.Consume(&nextRec);
            }
        }
        dr->outPipe->Insert(&tmpRec);
    }
    
    dr->outPipe->ShutDown();
    pthread_exit(NULL);
}

void DuplicateRemoval::WaitUntilDone() {
    pthread_join(worker, NULL);
}

void DuplicateRemoval::Use_n_Pages(int n) {
    this->n_pages = n;
}

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
    this->inPipe = &inPipe;
    this->outPipe = &outPipe;
    this->computeMe = &computeMe;
    pthread_create(&worker, NULL, startOperation, this);
}

void *Sum::startOperation(void *arg) {
    Sum *s = (Sum*)arg;
    Record outRec, tempRec;
    int intSum = 0;
    int intVal = 0;
    double doubleSum = 0.0;
    double doubleVal = 0.0;

    Attribute attr;
    attr.name = "SUM";
    stringstream output;

    if (s->computeMe->getReturnsInt() == 1) {
        Type valType = Int;
        while (s->inPipe->Remove(&tempRec)) {
            valType = s->computeMe->Apply(tempRec, intVal, doubleVal);
            intSum = intSum + intVal;
        }
        output << intSum << "|";
    }
    else if (s->computeMe->getReturnsInt() == 0) {
        Type valType = Double;
        while (s->inPipe->Remove(&tempRec)) {
            valType = s->computeMe->Apply(tempRec, intVal, doubleVal);
            doubleSum = doubleSum + doubleVal;
        }
        attr.myType = Double;
        output << doubleSum << "|";
    }
    else {
        cerr << "Error: Invalid type in Sum operation." << endl;
        exit(1);
    }

    Schema outSch("out_shema", 1, &attr);
    outRec.ComposeRecord(&outSch, output.str().c_str());
    s->outPipe->Insert(&outRec);

    s->outPipe->ShutDown();
    pthread_exit(NULL);
}

void Sum::WaitUntilDone() {
    pthread_join(worker, NULL);
}

void Sum::Use_n_Pages(int n) {
    this->n_pages = n;
}

void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
    this->inPipe = &inPipe;
    this->outPipe = &outPipe;
    this->groupAtts = &groupAtts;
    this->computeMe = &computeMe;
    pthread_create(&worker, NULL, startOperation, this);
}

void *GroupBy::startOperation(void *arg) {
    GroupBy* groupBy = (GroupBy*)arg;
    ComparisonEngine comparisonEngine;
    Record outRec, tempRec, lastRec;
    int intSum = 0;
    int intVal = 0;
    double doubleSum = 0.0;
    double doubleVal = 0.0;
    Type valType = String;

    Pipe outSort(100);
    BigQ bq = BigQ(*(groupBy->inPipe), outSort, *(groupBy->groupAtts), groupBy->n_pages);

    if (!outSort.Remove(&tempRec)) {
        groupBy->outPipe->ShutDown();
        pthread_exit(NULL);
    }

    valType = groupBy->computeMe->Apply(tempRec, intVal, doubleVal);
    if (valType == Int) {
        intSum = intSum + intVal;
    }
    else if (valType == Double) {
        doubleSum = doubleSum + doubleVal;
    }
    lastRec.Consume(&tempRec);

    while (outSort.Remove(&tempRec)) {
        if (comparisonEngine.Compare(&lastRec, &tempRec, groupBy->groupAtts)) {
            Attribute* attrs = new Attribute[groupBy->groupAtts->numAtts + 1];
            attrs[0].name = "SUM";
            stringstream output;
            if (valType == Int) {
                attrs[0].myType = Int;
                output << intSum << "|";
            }
            else if (valType == Double) {
                attrs[0].myType = Double;
                output << doubleSum << "|";
            }

            for (int i = 0; i < groupBy->groupAtts->numAtts; ++i) {
                Type curAttType = groupBy->groupAtts->whichTypes[i];
                if (curAttType == Int) {
                    attrs[i + 1].name = "int";
                    attrs[i + 1].myType = Int;
                    int val = *((int*)(lastRec.bits + ((int *) lastRec.bits)[groupBy->groupAtts->whichAtts[i] + 1]));
                    output << val << "|";
                }
                else if (curAttType == Double) {
                    attrs[i + 1].name = "double";
                    attrs[i + 1].myType = Double;
                    double val = *((double*)(lastRec.bits + ((int *) lastRec.bits)[groupBy->groupAtts->whichAtts[i] + 1]));
                    output << val << "|";
                }
                else {
                    attrs[i + 1].name = "string";
                    attrs[i + 1].myType = String;
                    string val = lastRec.bits + ((int *) lastRec.bits)[groupBy->groupAtts->whichAtts[i] + 1];
                    output << val << "|";
                }
            }

            Schema outSch("out_shema", groupBy->groupAtts->numAtts + 1, attrs);
            outRec.ComposeRecord(&outSch, output.str().c_str());
            groupBy->outPipe->Insert(&outRec);

            intSum = 0;
            intVal = 0;
            doubleSum = 0.0;
            doubleVal = 0.0;
        }

        valType = groupBy->computeMe->Apply(tempRec, intVal, doubleVal);
        if (valType == Int) {
            intSum = intSum + intVal;
        }
        else if (valType == Double) {
            doubleSum = doubleSum + doubleVal;
        }
        lastRec.Consume(&tempRec);
    }

    Attribute* attrs = new Attribute[groupBy->groupAtts->numAtts + 1];
    attrs[0].name = "SUM";
    stringstream output;
    if (valType == Int) {
        attrs[0].myType = Int;
        output << intSum << "|";
    }
    else if (valType == Double) {
        attrs[0].myType = Double;
        output << doubleSum << "|";
    }

    for (int i = 0; i < groupBy->groupAtts->numAtts; ++i) {
        Type curAttType = groupBy->groupAtts->whichTypes[i];
        if (curAttType == Int) {
            attrs[i + 1].name = "int";
            attrs[i + 1].myType = Int;
            int val = *((int*)(lastRec.bits + ((int *) lastRec.bits)[groupBy->groupAtts->whichAtts[i] + 1]));
            output << val << "|";
        }
        else if (curAttType == Double) {
            attrs[i + 1].name = "double";
            attrs[i + 1].myType = Double;
            double val = *((double*)(lastRec.bits + ((int *) lastRec.bits)[groupBy->groupAtts->whichAtts[i] + 1]));
            output << val << "|";
        }
        else {
            attrs[i + 1].name = "string";
            attrs[i + 1].myType = String;
            string val = lastRec.bits + ((int *) lastRec.bits)[groupBy->groupAtts->whichAtts[i] + 1];
            output << val << "|";
        }
    }

    Schema outSch("out_shema", groupBy->groupAtts->numAtts + 1, attrs);
    outRec.ComposeRecord(&outSch, output.str().c_str());
    groupBy->outPipe->Insert(&outRec);

    groupBy->outPipe->ShutDown();
    pthread_exit(NULL);
}

void GroupBy::WaitUntilDone() {
    pthread_join(worker, NULL);
}

void GroupBy::Use_n_Pages(int n) {
    this->n_pages = n;
}

void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema) {
    this->inPipe = &inPipe;
    this->outFile = outFile;
    this->mySchema = &mySchema;
    pthread_create(&worker, NULL, startOperation, this);
}

void *WriteOut::startOperation(void *arg) {
    WriteOut *wo = (WriteOut*)arg;
    Record tempRec;

    while (wo->inPipe->Remove(&tempRec)) {
        wo->writeOut(tempRec);
    }
    pthread_exit(NULL);
}

void WriteOut::writeOut(Record &rec) {
    int numAtts = mySchema->GetNumAtts();
    Attribute *atts = mySchema->GetAtts();

    for (int i = 0; i < numAtts; i++) {

        int pointer = ((int *)rec.bits)[i + 1];

        if (atts[i].myType == Int) {
            int *myInt = (int *) &(rec.bits[pointer]);
            fprintf(outFile, "%d", *myInt);
        }
        else if (atts[i].myType == Double) {
            double *myDouble = (double *) &(rec.bits[pointer]);
            fprintf(outFile, "%f", *myDouble);
        }
        else if (atts[i].myType == String) {
            char *myString = (char *) &(rec.bits[pointer]);
            fprintf(outFile, "%s", myString);
        }
        fprintf(outFile, "|");
    }

    fprintf(outFile, "\n");
}

void WriteOut::WaitUntilDone() {
    pthread_join(worker, NULL);
}

void WriteOut::Use_n_Pages(int n) {
    this->n_pages = n;
}
