#include "Statistics.h"
#include <iomanip>

AttrData :: AttrData () {}

AttrData :: AttrData (string name, int num) {
    attributeName = name;
    distinctValues = num;
}

AttrData :: AttrData (const AttrData &copyMe) {
    attributeName = copyMe.attributeName;
    distinctValues = copyMe.distinctValues;
}

AttrData &AttrData :: operator= (const AttrData &copyMe) {
    attributeName = copyMe.attributeName;
    distinctValues = copyMe.distinctValues;

    return *this;
}

RelData :: RelData () {
    isJoint = false;
}

RelData :: RelData (string name, int tuples) {
    isJoint = false;
    relName = name;
    totalTuples = tuples;
}

RelData :: RelData (const RelData &copyMe) {
    isJoint = copyMe.isJoint;
    relName = copyMe.relName;
    totalTuples = copyMe.totalTuples;
    attributeMap.insert (copyMe.attributeMap.begin (), copyMe.attributeMap.end ());
}

RelData &RelData :: operator= (const RelData &copyMe) {
    isJoint = copyMe.isJoint;
    relName = copyMe.relName;
    totalTuples = copyMe.totalTuples;
    attributeMap.insert (copyMe.attributeMap.begin (), copyMe.attributeMap.end ());

    return *this;
}

bool RelData :: doesRelExist (string relName) {
    if (relName == relName) {
        return true;
    }

    auto it = jointRelations.find(relName);

    if (it != jointRelations.end ()) {
        return true;
    }

    return false;
}

Statistics :: Statistics () {}

Statistics :: Statistics (Statistics &copyMe) {
    relationMap.insert (copyMe.relationMap.begin (), copyMe.relationMap.end ());
}

Statistics :: ~Statistics () {}

Statistics Statistics :: operator= (Statistics &copyMe) {
    relationMap.insert (copyMe.relationMap.begin (), copyMe.relationMap.end ());
    return *this;
}

double Statistics :: AndOp (AndList *andList, char *relName[], int numJoin) {
    if (andList == NULL) {
        return 1.0;
    }

    double left = 1.0;
    double right = 1.0;

    left = OrOp (andList->left, relName, numJoin);
    right = AndOp (andList->rightAnd, relName, numJoin);

//	cout << "left of and : " << left << endl;
//	cout << "right of and : " << right << endl;

    return left * right;
}

double Statistics :: OrOp (OrList *orList, char *relName[], int numJoin) {
    if (orList == NULL) {
        return 0.0;
    }

    double left = 0.0;
    double right = 0.0;

    left = ComOp (orList->left, relName, numJoin);
    int count = 1;

    OrList *tempOrList = orList->rightOr;
    char *attrName = orList->left->left->value;

    while (tempOrList) {
        if (strcmp (tempOrList->left->left->value, attrName) == 0) {
            count++;
        }
        tempOrList = tempOrList->rightOr;
    }

    if (count > 1) {
        return (double) count * left;
    }

    right = OrOp (orList->rightOr, relName, numJoin);

//	cout << "Left of Or : " << left << endl;
//	cout << "Right of Or : " << right << endl;

    return (double) (1.0 - (1.0 - left) * (1.0 - right));
}

double Statistics :: ComOp (ComparisonOp *compOp, char *relName[], int numJoin) {
    RelData leftRel, rightRel;
    double left = 0.0;
    double right = 0.0;

    int lResult = getRelationForOp(compOp->left, relName, numJoin, leftRel);
    int rResult = getRelationForOp(compOp->right, relName, numJoin, rightRel);
    int code = compOp->code;

    if (compOp->left->code == NAME) {
        if (lResult == -1) {
            cout << compOp->left->value << " does not belong to any relation!" << endl;
            left = 1.0;
        } else {
            string buffer (compOp->left->value);
            left = leftRel.attributeMap[buffer].distinctValues;
        }
    } else {
        left = -1.0;
    }

    if (compOp->right->code == NAME) {
        if (rResult == -1) {
            cout << compOp->right->value << " does not belong to any relation!" << endl;
            right = 1.0;
        } else {
            string buffer (compOp->right->value);
            right = rightRel.attributeMap[buffer].distinctValues;
        }
    } else {
        right = -1.0;
    }

    if (code == LESS_THAN || code == GREATER_THAN) {
        return 1.0 / 3.0;
    } else if (code == EQUALS) {
        if (left > right) {
            return 1.0 / left;
        } else {
            return 1.0 / right;
        }
    }
    cout << "Error!" << endl;
    return 0.0;
}

int Statistics :: getRelationForOp (Operand *operand, char **relName, int numJoin, RelData &relInfo) {
    if (operand == NULL) {
        return -1;
    }

    if (relName == NULL) {
        return -1;
    }

    for (auto iter = relationMap.begin (); iter != relationMap.end (); iter++) {
        string tempStr (operand->value);
        if (iter->second.attributeMap.find (tempStr) != iter->second.attributeMap.end()) {
            relInfo = iter->second;
            return 0;
        }
    }
    return -1;
}

void Statistics :: AddRel (char *relName, int numTuples) {
    string relStr (relName);
    RelData temp(relStr, numTuples);
    relationMap[relStr] = temp;
}

void Statistics :: AddAtt (char *relName, char *attrName, int numDistincts) {
    string relStr (relName);
    string attrStr (attrName);
    AttrData temp(attrStr, numDistincts);
    relationMap[relStr].attributeMap[attrStr] = temp;
}

void Statistics :: CopyRel (char *oldName, char *newName) {
    string previousStr (oldName);
    string currentStr (newName);
    relationMap[currentStr] = relationMap[previousStr];
    relationMap[currentStr].relName = currentStr;
    AttributeMap newAttrMap;
    for (auto it = relationMap[currentStr].attributeMap.begin (); it != relationMap[currentStr].attributeMap.end (); it++) {
        string newAttrStr = currentStr;
        newAttrStr.append (".");
        newAttrStr.append(it->first);
        AttrData tempAttribute (it->second);
        tempAttribute.attributeName = newAttrStr;
        newAttrMap[newAttrStr] = tempAttribute;
    }
    relationMap[currentStr].attributeMap = newAttrMap;
}

void Statistics :: Read (char *fromWhere) {
    int totalRelations, sizeOfJoint, totalAttributes, totalTuples, distinctValues;
    string relName, jointName, attributeName;
    ifstream in (fromWhere);
    if (!in) {
        cout << "File \"" << fromWhere << "\" does not exist!" << endl;
    }

    relationMap.clear ();
    in >> totalRelations;
    for (int i = 0; i < totalRelations; i++) {
        in >> relName;
        in >> totalTuples;
        RelData relation (relName, totalTuples);
        relationMap[relName] = relation;
        in >> relationMap[relName].isJoint;
        if (relationMap[relName].isJoint) {
            in >> sizeOfJoint;
            for (int j = 0; j < sizeOfJoint; j++) {
                in >> jointName;
                relationMap[relName].jointRelations[jointName] = jointName;
            }
        }
        in >> totalAttributes;
        for (int j = 0; j < totalAttributes; j++) {
            in >> attributeName;
            in >> distinctValues;
            AttrData attrInfo (attributeName, distinctValues);
            relationMap[relName].attributeMap[attributeName] = attrInfo;
        }
    }
}

void Statistics :: Write (char *toWhere) {
    ofstream out (toWhere);
    out << relationMap.size () << endl;
    for (auto iter = relationMap.begin (); iter != relationMap.end (); iter++ ) {
        //int num = iter->second.numTuples;
        out << iter->second.relName << endl;
        //out << std::fixed << iter->second.numTuples << endl;
        out << iter->second.totalTuples << endl;
        out << iter->second.isJoint << endl;
        if (iter->second.isJoint) {
            out << iter->second.jointRelations.size () << endl;
            for (auto it = iter->second.jointRelations.begin (); it != iter->second.jointRelations.end (); it++ ) {
                out << it->second << endl;
            }
        }
        out << iter->second.attributeMap.size () << endl;

        for (auto it = iter->second.attributeMap.begin (); it != iter->second.attributeMap.end ();it++) {
            out << it->second.attributeName << endl;
            out << it->second.distinctValues << endl;
        }
    }
    out.close ();
}

void Statistics :: Apply (struct AndList *parseTree, char *relNames[], int numToJoin) {
    int idx = 0;
    int joinCount = 0;
    char *relationNames[100];
    RelData relData;
    while (idx < numToJoin) {
        string temp (relNames[idx]);
        auto iter = relationMap.find (temp);
        if (iter != relationMap.end ()) {
            relData = iter->second;
            relationNames[joinCount++] = relNames[idx];
            if (relData.isJoint) {
                int size = relData.jointRelations.size();
                if (size <= numToJoin) {
                    for (int i = 0; i < numToJoin; i++) {
                        string buf (relNames[i]);
                        if (relData.jointRelations.find (buf) == relData.jointRelations.end () &&
                            relData.jointRelations[buf] != relData.jointRelations[temp]) {
                            cout << "Cannot be joined!" << endl;
                            return;
                        }
                    }
                } else {
                    cout << "Cannot be joined!" << endl;
                }
            } else {
                idx++;
                continue;
            }
        }
        idx++;
    }

    double estimation = Estimate (parseTree, relationNames, joinCount);

    idx = 1;
    string firstRelName (relationNames[0]);
    RelData firstRel = relationMap[firstRelName];
    RelData tempRel;

    relationMap.erase (firstRelName);
    firstRel.isJoint = true;
    firstRel.totalTuples = estimation;

    while (idx < joinCount) {
        string temp (relationNames[idx]);
        firstRel.jointRelations[temp] = temp;
        tempRel = relationMap[temp];
        relationMap.erase (temp);
        firstRel.attributeMap.insert (tempRel.attributeMap.begin (), tempRel.attributeMap.end ());
        idx++;
    }
    relationMap.insert (pair<string, RelData> (firstRelName, firstRel));
}

double Statistics :: Estimate (struct AndList *parseTree, char **relNames, int numToJoin) {
    int index = 0;
    double factor = 1.0;
    double product = 1.0;

    while (index < numToJoin) {
        string temp (relNames[index]);
        if (relationMap.find (temp) != relationMap.end ()) {  //If temp found in map
            product *= (double) relationMap[temp].totalTuples;
        }
        index++;
    }

    if (parseTree == NULL) {
        return product;
    }
    factor = AndOp (parseTree, relNames, numToJoin);
    return factor * product;
}