#ifndef STATISTICS_H
#define STATISTICS_H
#include "ParseTree.h"
#include <map>
#include <string>
#include <vector>
#include <utility>
#include <cstring>
#include <fstream>
#include <iostream>

using namespace std;

class AttrData;
class RelData;

typedef map<string, AttrData> AttributeMap;
typedef map<string, RelData> RelationMap;

class AttrData {

public:

    string attributeName;
    int distinctValues;

    AttrData ();
    AttrData (string name, int num);
    AttrData (const AttrData &copyMe);

    AttrData &operator= (const AttrData &copyMe);

};

class RelData {

public:

    double totalTuples;

    bool isJoint;		// If a relation is joint with any other relation or not
    string relName;

    AttributeMap attributeMap;		// holds all the attributes in this relation
    map<string, string> jointRelations;		// holds which all relations this relation is joint with

    RelData ();
    RelData (string name, int tuples);
    RelData (const RelData &copyMe);

    RelData &operator= (const RelData &copyMe);

    bool doesRelExist (string relName);

};

class Statistics {

private:
    double AndOp (AndList *andList, char *relName[], int numJoin);
    double OrOp (OrList *orList, char *relName[], int numJoin);
    double ComOp (ComparisonOp *comOp, char *relName[], int numJoin);
    int getRelationForOp (Operand *operand, char **relName, int numJoin, RelData &relInfo);

public:

    RelationMap relationMap;		// Holds the relations that are currently in the statistic file.

    Statistics();
    Statistics(Statistics &copyMe);	 // Performs deep copy
    ~Statistics();

    Statistics operator= (Statistics &copyMe);

    void AddRel(char *relName, int numTuples);
    void AddAtt(char *relName, char *attrName, int numDistincts);
    void CopyRel(char *oldName, char *newName);

    void Read(char *fromWhere);
    void Write(char *toWhere);

    void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
    double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

    bool isRelInMap (string relName, RelData &relInfo);

};

#endif
