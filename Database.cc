
#include <stdio.h>
#include <string>
#include "Database.h"

using namespace std;

extern "C" {
int yyparse(void);   // defined in y.tab.c
}

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

extern int sqlType; // 0 for create, 1 for insert, 2 for drop, 3 for set, 4 for select, 5 for update
extern int dbfileType; // 0 for heap file, 1 for sorted file, 2 for B plus tree file
extern string tablename;
extern string loadFileName; // save the insert file name
extern string outputFile; // it could be none, string or stdout
extern struct AttrList *attrList;
extern struct NameList *sortList;

void Database :: makePermutationsHelper(vector<string> &foundTables, int index, vector<vector<string>> &resultVec, vector<string> &tempRes) {
    if(index == foundTables.size())
    {
        resultVec.push_back(tempRes);
        return;
    }
    for(int i = index; i < foundTables.size(); i++)
    {
        swap(foundTables[index], foundTables[i]);
        tempRes.push_back(foundTables[index]);
        makePermutationsHelper(foundTables, index + 1, resultVec, tempRes);
        tempRes.pop_back();
        swap(foundTables[index], foundTables[i]);
    }
}

vector<vector<string>> Database :: getPermutations(vector<string> &seenTable) {
    vector<vector<string>> res;
    vector<string> tmpres;
    makePermutationsHelper(seenTable, 0, res, tmpres);
    return res;
}

void Database :: traverse(OpTreeNode *currNode, int outputMode) {
    if (!currNode)
        return;
    switch (currNode->getType()) {
        case SELECTFILE:
            if (outputMode == 0) ((SelectFileNode*)currNode)->print();
            else ((SelectFileNode*)currNode)->run();
            break;
        case SELECTPIPE:
            traverse(((SelectPipeNode*)currNode)->left, outputMode);
            if (outputMode == 0) ((SelectPipeNode*)currNode)->print();
            else ((SelectPipeNode*)currNode)->run();
            break;
        case PROJECT:
            traverse(((ProjectNode*)currNode)->left, outputMode);
            if (outputMode == 0) ((ProjectNode*)currNode)->print();
            else ((ProjectNode*)currNode)->run();
            break;
        case GROUPBY:
            traverse(((GroupByNode*)currNode)->left, outputMode);
            if (outputMode == 0) ((GroupByNode*)currNode)->print();
            else ((GroupByNode*)currNode)->run();
            break;
        case SUM:
            traverse(((SumNode*)currNode)->left, outputMode);
            if (outputMode == 0) ((SumNode*)currNode)->print();
            else ((SumNode*)currNode)->run();
            break;
        case DUPLICATEREMOVAL:
            traverse(((DuplicateRemovalNode*)currNode)->left, outputMode);
            if (outputMode == 0) ((DuplicateRemovalNode*)currNode)->print();
            else ((DuplicateRemovalNode*)currNode)->run();
            break;
        case JOIN:
            traverse(((JoinNode*)currNode)->left, outputMode);
            traverse(((JoinNode*)currNode)->right, outputMode);
            if (outputMode == 0) ((JoinNode*)currNode)->print();
            else ((JoinNode*)currNode)->run();
            break;
        default:
            cerr << endl << "ERROR: Undefined operation !!! " << endl;
            exit(-1);
    }
    if (outputMode == 0) cout << endl << "*******************************************************" << endl;
}

void Database :: copySchema(unordered_map<string, Schema*> &aliasSchemas, char* oldName, char* newName) {

    Attribute *previousAttributes = schemaMap[oldName]->GetAtts();
    int numberOfAttributes = schemaMap[oldName]->GetNumAtts();
    Attribute *newAttributes = new Attribute[numberOfAttributes];
    size_t length = strlen(newName);

    for (int i = 0; i < numberOfAttributes; ++i) {
        size_t attributeLength = strlen(previousAttributes[i].name);
        newAttributes[i].name = new char[attributeLength + length + 2];
        strcpy(newAttributes[i].name, newName);
        strcat(newAttributes[i].name, ".");
        strcat(newAttributes[i].name, previousAttributes[i].name);
        newAttributes[i].myType = previousAttributes[i].myType;
    }
    aliasSchemas[newName] = new Schema(newName, numberOfAttributes, newAttributes);
}

OpTreeNode * Database :: buildOperationTree(unordered_map<string, Schema*> &aliasSchemas, unordered_map<string, string> &aliasName, vector<string> &optimumJoinOrder, int numberOfRelations ){

    OpTreeNode *left = new SelectFileNode(boolean, aliasSchemas[optimumJoinOrder[0]], aliasName[optimumJoinOrder[0]]);
    OpTreeNode *root = left;

    for (int i = 1; i < numberOfRelations; ++i) {
        OpTreeNode *right = new SelectFileNode(boolean, aliasSchemas[optimumJoinOrder[i]], aliasName[optimumJoinOrder[i]]);
        root = new JoinNode(left, right, boolean);
        left = root;
    }
    if (distinctAtts == 1 || distinctFunc == 1) {
        root = new DuplicateRemovalNode(left);
        left = root;
    }

    // OrderMaker groupOrder;
    if (groupingAtts) {
        /*NameList * grpAtts = groupingAtts;
        Schema *beforeDistinctSch = left->getSchema();

        if(distinctAtts == 1 || distinctFunc == 1){
            root = new DuplicateRemovalNode(left);

///////////////////////////////////////////////////////////////////////////////////////////////

            Schema* inputSchema = left->getSchema();
            while (grpAtts) {
                groupOrder.whichAtts[groupOrder.numAtts] = inputSchema->Find(grpAtts->name);
                groupOrder.whichTypes[groupOrder.numAtts] = inputSchema->FindType(grpAtts->name);
                ++groupOrder.numAtts;
                grpAtts = grpAtts->next;
            }

            Function computeMe;
            computeMe.GrowFromParseTree(finalFunction, *left->getSchema());

            Attribute* atts = new Attribute[groupOrder.numAtts + 1];
            atts[0].name = "SUM";
            stringstream output;
            if (computeMe.returnsInt == 0) {
                atts[0].myType = Double;
            }
            else {
                atts[0].myType = Int;
            }
            Attribute* childAtts = left->getSchema()->GetAtts();
            for (int i = 0; i < groupOrder.numAtts; ++i) {
                atts[i + 1].name = childAtts[groupOrder.whichAtts[i]].name;
                atts[i + 1].myType = childAtts[groupOrder.whichAtts[i]].myType;
            }



            root->outputSchema = new Schema("out_shema", groupOrder.numAtts + 1, atts);


            /////////////////////////////////////////////////////////////////////////////////////////

            left = root;

            cout << "\nend of distinct\n";
        }
*/
        root = new GroupByNode(left, groupingAtts, finalFunction);
        left = root;
        NameList* sum = new NameList();
        sum->name = "SUM";
        sum->next = attsToSelect;
        root = new ProjectNode(left, sum);
    }
    else if (finalFunction) {
        root = new SumNode(left, finalFunction);
        left = root;
    }
    else if (attsToSelect) {
        root = new ProjectNode(left, attsToSelect);
    }
    return root;
}

void Database :: executeSelect() {
    statistics.setup();
    vector<string> foundTable;
    unordered_map<string, string> aliasName;
    unordered_map<string, Schema*> aliasSchemas;

    TableList *cur = tables;
    while (cur) {
        if (schemaMap.count(cur->tableName) == 0) {
            cerr << endl << "Error: " << cur->tableName <<" table does not exist in the database " << endl;
            return;
        }
        statistics.CopyRel(cur->tableName, cur->aliasAs);
        copySchema(aliasSchemas, cur->tableName, cur->aliasAs);
        foundTable.push_back(cur->aliasAs);
        aliasName[cur->aliasAs] = cur->tableName;
        cur = cur->next;
    }

    statistics.Write(stats_dir.c_str());

    vector<vector<string>> joinOrder = getPermutations(foundTable);
    int optimumJoinIndex = 0;
    double minEstimate = DBL_MAX;
    size_t numberOfRelations = joinOrder[0].size();
    if (numberOfRelations == 1) {
        char **relNames = new char*[1];
        relNames[0] = new char[joinOrder[0][0].size() + 1];
        strcpy(relNames[0], joinOrder[0][0].c_str());
        minEstimate = statistics.Estimate(boolean, relNames, 1);
    }
    else {
        for (int i = 0; i < joinOrder.size(); ++i) {
            statistics.Read(stats_dir.c_str());
            double result = 0;
            char **relNames = new char*[numberOfRelations];
            for (int j = 0; j < numberOfRelations; ++j) {
                relNames[j] = new char[joinOrder[i][j].size() + 1];
                strcpy(relNames[j], joinOrder[i][j].c_str());
            }

            for (int j = 2; j <= numberOfRelations; ++j) {
                result += statistics.Estimate(boolean, relNames, j);
                statistics.Apply(boolean, relNames, j);
            }

            if (result < minEstimate) {
                minEstimate = result;
                optimumJoinIndex = i;
            }
        }
    }

    vector<string> optimumJoinOrder = joinOrder[optimumJoinIndex];

    /*
     * Generate opTree
     */

    OpTreeNode * root = buildOperationTree(aliasSchemas, aliasName, optimumJoinOrder, numberOfRelations);


    if (outputMode == 0) {
        cout << endl << "Estimated Query Plan tree :" << endl;
        cout << endl << "*******************************************************" << endl;
        traverse(root, 0);
    }
    else if (outputMode == 1){
        traverse(root, 1);
        Record rec;
        while (root->outpipe.Remove(&rec)) {
            rec.Print(root->getSchema());
        }
    }
    else {
        traverse(root, 2);
        WriteOut writeOut;
        string outputPath = output_dir + outputFile;
        FILE *outFile = fopen(outputPath.c_str(), "w");
        writeOut.Run(root->outpipe, outFile, *root->getSchema());
        writeOut.WaitUntilDone();
        fclose(outFile);
    }
}

void Database :: Execute() {

    cout << endl << "Enter SQL command (Then press return and Control-D): " << endl;
    if (yyparse()) {
        cerr << endl << "Error: Unable to parse the provided SQL!" << endl;
        cerr << "Note: Please make sure there is a semicolon (;) at the end of the SQL command" << endl;
        return;
    };


    switch (sqlType) {
        case 0: {
            if (tableMap.count(tablename) != 0) {     // create table
                cerr << endl << "Error: " <<tablename<<" table already exists in current database!" << endl;
                return;
            }

            int numberOfAttributes = 0;
            AttrList *cur = attrList;
            while (cur) {
                ++numberOfAttributes;
                cur = cur->next;
            }
            cur = attrList;
            Attribute *atts = new Attribute[numberOfAttributes];
            for (int i = 0; i < numberOfAttributes; ++i) {
                atts[i].name = cur->name;
                switch (cur->type) {
                    case 0: {
                        atts[i].myType = Int;
                    }
                        break;
                    case 1: {
                        atts[i].myType = Double;
                    }
                        break;
                    case 2: {
                        atts[i].myType = String;
                    }
                        break;
                    default: {
                        cerr << endl << "Error: Invalid data type for the attribute!" << endl;
                    }
                }
                cur = cur->next;
            }
            Schema *mySchema = new Schema(tablename.c_str(), numberOfAttributes, atts);
            schemaMap[tablename] = mySchema;

            if (dbfileType == 0) {
                DBFile dbfile;
                string filepath = "database/" + tablename + ".bin";
                dbfile.Create(filepath.c_str(), heap, nullptr);
                dbfile.Close();
                tableMap[tablename] = heap;
            }
            else {
                OrderMaker sortOrder;
                int nSortAtts = 0;
                NameList *nameList = sortList;
                while (nameList) {
                    sortOrder.whichAtts[nSortAtts] = mySchema->Find(nameList->name);
                    sortOrder.whichTypes[nSortAtts] = mySchema->FindType(nameList->name);
                    ++nSortAtts;
                    nameList = nameList->next;
                }
                sortOrder.numAtts = nSortAtts;
                SortInfo sortInfo;
                sortInfo.sortOrder = &sortOrder;
                sortInfo.runLength = 5;

                DBFile dbfile;
                string filepath = "database/" + tablename + ".bin";
                dbfile.Create(filepath.c_str(), sorted, &sortInfo);
                dbfile.Close();
                tableMap[tablename] = sorted;
            }
            ++tableCount;
            cout << endl << tablename << " table created. Total number of tables in database = " << tableCount << endl;
        }
            break;
        case 1: {       // insert
            if (tableMap.count(tablename) == 0) {
                cerr << endl << "Error: "<<tablename<<" table does not exist in the database!" << endl;
                return;
            }
            DBFile dbfile;
            string filepath = "database/" + tablename + ".bin";
            dbfile.Open(filepath.c_str());
            string loadPath = tpch_dir + loadFileName;
            dbfile.Load(*schemaMap[tablename], loadPath.c_str());
            dbfile.Close();
        }
            break;
        case 2: {       // DROP Table
            if (tableMap.count(tablename) == 0) {
                cerr << endl << "Error: "<<tablename<<" table does not exist in the database!" << endl;
                return;
            }
            tableMap.erase(tablename);
            schemaMap.erase(tablename);
            --tableCount;
            cout << endl << tablename << " table dropped from the database. Total number of tables in database = " << tableCount << endl;
        }
            break;
        case 3: {       // SET OUTPUT
            if (outputFile == "NONE") {
                outputMode = 0;
            }
            else if (outputFile == "STDOUT") {
                outputMode = 1;
            }
            else {
                outputMode = 2;
            }
        }
            cout << "\nOutput mode set to " << outputFile << endl;
            break;
        case 4: {       // SELECT
            executeSelect();
        }
            break;
        default: {
            cerr << endl << "Error: Unidentified SQL statement!" << endl;
        }
    }
}

void Database :: Create() {
    ofstream file(dbMetadata, ios::trunc);
    file << 0;
    file.close();
    file.open(catalog_dir, ios::trunc);
    file.close();
    tableCount = 0;
    tableMap.clear();
    schemaMap.clear();
}

void Database :: Open() {
    ifstream file(dbMetadata);
    file >> tableCount;
    for (int i = 0; i < tableCount; ++i) {
        string colName = "";
        file >> colName;
        int type = 0;
        file >> type;
        tableMap[colName] = type;
        schemaMap[colName] = new Schema(catalog_dir.c_str(), colName.c_str());
    }
    file.close();
}

void Database :: Close() {
    ofstream file(dbMetadata, ios::trunc);
    file << tableCount << endl;
    for (auto iter = tableMap.begin(); iter != tableMap.end(); ++iter) {
        file << iter->first << " " << iter->second << endl;
    }
    file.close();
    file.open(catalog_dir, ios::trunc);
    for (auto i = schemaMap.begin(); i != schemaMap.end(); ++i) {
        file << endl;
        file << "BEGIN" << endl;
        file << i->first << endl;
        file << i->first + ".tbl" << endl;
        int numberOfAttributes = i->second->GetNumAtts();
        Attribute *atts = i->second->GetAtts();
        for (int j = 0; j < numberOfAttributes; ++j) {
            switch (atts[j].myType) {
                case Int: {
                    file << atts[j].name << " Int" << endl;
                }
                    break;
                case Double: {
                    file << atts[j].name << " Double" << endl;
                }
                    break;
                case String: {
                    file << atts[j].name << " String" << endl;
                }
                    break;
                default: {
                    cerr << "Error: Invalid type!" << endl;
                    exit(-1);
                }
            }
        }
        file << "END" << endl;
    }
    file.close();
}

void Database ::showTables() {
    cout << "\n\nTables in database are:\n\n";
    for(auto i = tableMap.begin(); i != tableMap.end(); i++){
        cout << i->first << " : ";
        if(i->second == 0){
            cout << "Heap\n";
        }
        else{
            cout << "Sorted\n";
        }
    }
    cout << "\n\n";
}