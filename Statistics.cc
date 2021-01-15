#include "Statistics.h"

using namespace std;

Statistics::Statistics(Statistics &copyMe)
{
    relationsMap = unordered_map<string, int>(copyMe.relationsMap);
    relAttributes = unordered_map<string, pair<string, int>>(copyMe.relAttributes);
}

void Statistics::setup() {

    relationsMap.clear();
    relAttributes.clear();

    const char *supplier = "supplier";
    const char *partsupp = "partsupp";
    const char *part = "part";
    const char *nation = "nation";
    const char *customer = "customer";
    const char *orders = "orders";
    const char *region = "region";
    const char *lineitem = "lineitem";

    AddRel(region,5);
    AddRel(nation,25);
    AddRel(part,200000);
    AddRel(supplier,10000);
    AddRel(partsupp,800000);
    AddRel(customer,150000);
    AddRel(orders,1500000);
    AddRel(lineitem,6001215);

    // region
    AddAtt(region, "r_regionkey",5); // r_regionkey=5
    AddAtt(region, "r_name",5); // r_name=5
    AddAtt(region, "r_comment",5); // r_comment=5
    // nation
    AddAtt(nation, "n_nationkey",25); // n_nationkey=25
    AddAtt(nation, "n_name",25);  // n_name=25
    AddAtt(nation, "n_regionkey",5);  // n_regionkey=5
    AddAtt(nation, "n_comment",25);  // n_comment=25
    // part
    AddAtt(part, "p_partkey",200000); // p_partkey=200000
    AddAtt(part, "p_name",200000); // p_name=199996
    AddAtt(part, "p_mfgr",200000); // p_mfgr=5
    AddAtt(part, "p_brand",200000); // p_brand=25
    AddAtt(part, "p_type",200000); // p_type=150
    AddAtt(part, "p_size",200000); // p_size=50
    AddAtt(part, "p_container",200000); // p_container=40
    AddAtt(part, "p_retailprice",200000); // p_retailprice=20899
    AddAtt(part, "p_comment",200000); // p_comment=127459
    // supplier
    AddAtt(supplier,"s_suppkey",10000);
    AddAtt(supplier,"s_name",10000);
    AddAtt(supplier,"s_address",10000);
    AddAtt(supplier,"s_nationkey",25);
    AddAtt(supplier,"s_phone",10000);
    AddAtt(supplier,"s_acctbal",9955);
    AddAtt(supplier,"s_comment",10000);
    // partsupp
    AddAtt(partsupp,"ps_partkey",200000);
    AddAtt(partsupp,"ps_suppkey",10000);
    AddAtt(partsupp,"ps_availqty",9999);
    AddAtt(partsupp,"ps_supplycost",99865);
    AddAtt(partsupp,"ps_comment",799123);
    // customer
    AddAtt(customer,"c_custkey",150000);
    AddAtt(customer,"c_name",150000);
    AddAtt(customer,"c_address",150000);
    AddAtt(customer,"c_nationkey",25);
    AddAtt(customer,"c_phone",150000);
    AddAtt(customer,"c_acctbal",140187);
    AddAtt(customer,"c_mktsegment",5);
    AddAtt(customer,"c_comment",149965);
    // orders
    AddAtt(orders,"o_orderkey",1500000);
    AddAtt(orders,"o_custkey",99996);
    AddAtt(orders,"o_orderstatus",3);
    AddAtt(orders,"o_totalprice",1464556);
    AddAtt(orders,"o_orderdate",2406);
    AddAtt(orders,"o_orderpriority",5);
    AddAtt(orders,"o_clerk",1000);
    AddAtt(orders,"o_shippriority",1);
    AddAtt(orders,"o_comment",1478684);
    // lineitem
    AddAtt(lineitem,"l_orderkey",1500000);
    AddAtt(lineitem,"l_partkey",200000);
    AddAtt(lineitem,"l_suppkey",10000);
    AddAtt(lineitem,"l_linenumber",7);
    AddAtt(lineitem,"l_quantity",50);
    AddAtt(lineitem,"l_extendedprice",933900);
    AddAtt(lineitem,"l_discount",11);
    AddAtt(lineitem,"l_tax",9);
    AddAtt(lineitem,"l_returnflag",3);
    AddAtt(lineitem,"l_linestatus",2);
    AddAtt(lineitem,"l_shipdate",2526);
    AddAtt(lineitem,"l_commitdate",2466);
    AddAtt(lineitem,"l_receiptdate",2554);
    AddAtt(lineitem,"l_shipinstruct",4);
    AddAtt(lineitem,"l_shipmode",7);
    AddAtt(lineitem,"l_comment",4501941);
}

void Statistics::AddAtt(const char *relName, const char *attName, int numDistincts)
{
    string RelName(relName);
    string AttName(attName);

    if (numDistincts == -1) {
        relAttributes[AttName].first = RelName;
        relAttributes[AttName].second = relationsMap[RelName];
    }
    else {
        relAttributes[AttName].first = RelName;
        relAttributes[AttName].second = numDistincts;
    }
}

void Statistics::AddRel(const char *relName, int numTuples)
{
    string RelName(relName);
    relationsMap[RelName] = numTuples;
}

void Statistics::Read(const char *fromWhere)
{
    ifstream statFile(fromWhere);

    if (!statFile) {
        cerr << "Statistics statFile doesn't exist!" << endl;
        exit(-1);
    }

    relationsMap.clear();
    relAttributes.clear();

    int numberOfAttributes;
    statFile >> numberOfAttributes;

    for (int i = 0; i < numberOfAttributes; i++) {
        string attribute;
        statFile >> attribute;

        string::size_type index1 = attribute.find_first_of(":");
        string::size_type index2 = attribute.find_last_of(":");
        string attributeName = attribute.substr(0, index1);
        string relationName = attribute.substr(index1 + 1, index2 - index1 - 1);
        int numDistincts = atoi(attribute.substr(index2 + 1).c_str());
        relAttributes[attributeName].first = relationName;
        relAttributes[attributeName].second = numDistincts;
    }

    int numberOfRelations;
    statFile >> numberOfRelations;

    for (int i = 0; i < numberOfRelations; i++) {
        string relation;
        statFile >> relation;

        string::size_type index = relation.find_first_of(":");
        string relName = relation.substr(0, index);
        int NumTuples = atoi(relation.substr(index + 1).c_str());
        relationsMap[relName] = NumTuples;
    }

    statFile.close();
}

void Statistics::Write(const char *fromWhere)
{
    ofstream statFile(fromWhere);

    statFile << relAttributes.size() << endl;

    for (auto i = relAttributes.begin(); i != relAttributes.end(); ++i) {
        statFile << i->first << ":" << i->second.first << ":" << i->second.second << endl;
    }

    statFile << relationsMap.size() << endl;

    for (auto j = relationsMap.begin(); j != relationsMap.end(); ++j) {
        statFile << j->first << ":" << j->second << endl;
    }

    statFile.close();
}

void Statistics::CopyRel(const char *oldName, const char *newName)
{
    string oldNameStr(oldName);
    string newNameStr(newName);

    relationsMap[newNameStr] = relationsMap[oldNameStr];
    unordered_map<string, pair<string, int>> newAttrMap;

    for (auto iter = relAttributes.begin(); iter != relAttributes.end(); ++iter) {
        if (iter->second.first == oldNameStr) {
            string newAttrName = newNameStr + "." + iter->first;
            newAttrMap[newAttrName].first = newNameStr;
            newAttrMap[newAttrName].second = iter->second.second;
        }
    }
    for (auto iter = newAttrMap.begin(); iter != newAttrMap.end(); ++iter) {
        AddAtt(newName, iter->first.c_str(), iter->second.second);
    }
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    double andRes = 1.0;
    vector<string> joinRels(2);
    string joinedRelName = "";
    for (int i = 0; i < numToJoin - 1; ++i) {
        joinedRelName += relNames[i];
    }
    if (relationsMap.count(joinedRelName) > 0) {
        joinRels[0] = joinedRelName;
        joinRels[1] = relNames[numToJoin - 1];
    }
    else {
        joinedRelName += relNames[numToJoin - 1];
        joinRels[0] = joinedRelName;
    }
    if (!parseTree) {
        return 0;
    }
    AndList *curAnd = parseTree;
    while (curAnd) {
        OrList *curOr = curAnd->left;
        double orRes = 1.0;
        unordered_map<string, double> orPairList;
        bool flag = false;
        while (curOr) {
            ComparisonOp *curComp = curOr->left;
            Operand *left = curComp->left;
            Operand *right = curComp->right;
            if (curComp->code == EQUALS) {
                if (left->code == NAME && right->code == NAME) {
                    string leftAttributeName = left->value;
                    string rightAttributeName = right->value;
                    string leftRelation = relAttributes[leftAttributeName].first;
                    string rightRelation = relAttributes[rightAttributeName].first;
                    if ((leftRelation != joinRels[0] && leftRelation != joinRels[1]) || (rightRelation != joinRels[0] && rightRelation != joinRels[1])) {
                        flag = true;
                        break;
                    }
                    int leftDistinct = relAttributes[leftAttributeName].second;
                    int rightDistinct = relAttributes[rightAttributeName].second;
                    orRes = 1.0 / max(leftDistinct, rightDistinct);
                }
                else {
                    if (left->code == NAME || right->code == NAME) {
                        string attributeName = (left->code == NAME) ? left->value : right->value;
                        string relationName = relAttributes[attributeName].first;
                        if (relationName != joinRels[0] && relationName != joinRels[1]) {
                            flag = true;
                            break;
                        }
                        orPairList[attributeName] += 1.0 / relAttributes[attributeName].second;
                        if (orPairList[attributeName] > 1.0) {
                            orPairList[attributeName] = 1.0;
                        }
                    }
                    else if (left->code == right->code && left->value == right->value) {
                        flag = true;
                        break;
                    }
                }
            }
            else {
                if (left->code == NAME || right->code == NAME) {
                    string attributeName = (left->code == NAME) ? left->value : right->value;
                    string relationName = relAttributes[attributeName].first;
                    if (relationName != joinRels[0] && relationName != joinRels[1]) {
                        flag = true;
                        break;
                    }
                    orPairList[attributeName] += 1.0 / 3.0;
                    if (orPairList[attributeName] > 1.0) {
                        orPairList[attributeName] = 1.0;
                    }
                }
                else if (left->code == right->code && ((curComp->code == GREATER_THAN && left->value > right->value) || (curComp->code == LESS_THAN && left->value < right->value))) {
                    flag = true;
                    break;
                }
            }
            curOr = curOr->rightOr;
        }
        if (!flag && !orPairList.empty()) {
            double reveRes = 1.0;
            for (auto iter = orPairList.begin(); iter != orPairList.end(); ++iter) {
                reveRes *= (1.0 - iter->second);
            }
            orRes = 1 - reveRes;
        }
        andRes *= orRes;
        curAnd = curAnd->rightAnd;
    }
    double result = 1.0;
    result *= relationsMap[joinRels[0]];
    if (joinRels[1] != "") result *= relationsMap[joinRels[1]];
    result *= andRes;
    return result;
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    double andRes = 1.0;
    int64_t modifier = 1;
    vector<string> joinRels(2);
    string jointRelationName = "";
    for (int i = 0; i < numToJoin - 1; ++i) {
        jointRelationName += relNames[i];
    }
    if (relationsMap.count(jointRelationName) > 0) {
        joinRels[0] = jointRelationName;
        joinRels[1] = relNames[numToJoin - 1];
    }
    else {
        jointRelationName += relNames[numToJoin - 1];
        joinRels[0] = jointRelationName;
    }

    if (!parseTree) {
        return;
    }

    AndList *curAnd = parseTree;
    while (curAnd) {
        OrList *curOr = curAnd->left;
        unordered_map<string, double> orPairList;
        bool flag = false;
        double orResult = 1.0;

        while (curOr) {
            ComparisonOp *curComp = curOr->left;
            Operand *left = curComp->left;
            Operand *right = curComp->right;

            if (curComp->code == EQUALS) {
                if (left->code == NAME && right->code == NAME) {
                    string lAttributeName = left->value;
                    string rAttributeName = right->value;

                    string lRelation = relAttributes[lAttributeName].first;
                    string rRelation = relAttributes[rAttributeName].first;

                    if ((lRelation != joinRels[0] && lRelation != joinRels[1]) || (rRelation != joinRels[0] && rRelation != joinRels[1])) {
                        flag = true;
                        break;
                    }

                    int lDistinct = relAttributes[lAttributeName].second;
                    int rDistinct = relAttributes[rAttributeName].second;

                    relAttributes[lAttributeName].second = min(lDistinct, rDistinct);
                    relAttributes[rAttributeName].second = min(lDistinct, rDistinct);

                    modifier = max(lDistinct, rDistinct);
                    orResult = 1.0 / modifier;
                }
                else {
                    if (left->code == NAME || right->code == NAME) {
                        string attributeName = (left->code == NAME) ? left->value : right->value;
                        string relationName = relAttributes[attributeName].first;
                        if (relationName != joinRels[0] && relationName != joinRels[1]) {
                            flag = true;
                            break;
                        }
                        int distinct = relAttributes[attributeName].second;

                        orPairList[attributeName] += 1.0 / distinct;
                        if (orPairList[attributeName] > 1.0) {
                            orPairList[attributeName] = 1.0;
                        }
                    }
                    else if (left->code == right->code && left->value == right->value) {
                        flag = true;
                        break;
                    }
                }
            }
            else {
                if (left->code == NAME || right->code == NAME) {
                    string attributeName = (left->code == NAME) ? left->value : right->value;
                    string relationName = relAttributes[attributeName].first;
                    if (relationName != joinRels[0] && relationName != joinRels[1]) {
                        flag = true;
                        break;
                    }
                    orPairList[attributeName] += 1.0 / 3.0;
                    if (orPairList[attributeName] > 1.0) {
                        orPairList[attributeName] = 1.0;
                    }
                }
                else if (left->code == right->code && ((curComp->code == GREATER_THAN && left->value > right->value) || (curComp->code == LESS_THAN && left->value < right->value))) {
                    flag = true;
                    break;
                }
            }
            curOr = curOr->rightOr;
        }
        if (!flag && !orPairList.empty()) {
            double reveRes = 1;
            for (auto iter = orPairList.begin(); iter != orPairList.end(); ++iter) {
                reveRes *= (1.0 - iter->second);
            }
            orResult = 1 - reveRes;
        }
        andRes *= orResult;
        curAnd = curAnd->rightAnd;
    }
    double numofTuples = andRes;
    numofTuples *= relationsMap[joinRels[0]];
    relationsMap.erase(joinRels[0]);
    if (joinRels[1] != "") {
        numofTuples *= relationsMap[joinRels[1]];
        relationsMap.erase(joinRels[1]);
    }
    string newRelationName = joinRels[0] + joinRels[1];
    relationsMap[newRelationName] = numofTuples;
    andRes *= modifier;
    for (auto iter = relAttributes.begin(); iter != relAttributes.end(); ++iter) {
        if (iter->second.first == joinRels[0] || iter->second.first == joinRels[1]) {
            string attrName = iter->first;
            int numofDistinct = iter->second.second * andRes;
            relAttributes[attrName].first = newRelationName;
            relAttributes[attrName].second = numofDistinct;
        }
    }
}