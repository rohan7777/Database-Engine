//
//  OpTreeNode.cc
//  COP6726_5
//
//  Created by Yihao Wu on 5/4/19.
//  Copyright © 2019 Yihao Wu. All rights reserved.
//

#include "OpTreeNode.h"

using namespace std;

SelectFileNode :: SelectFileNode(AndList *selectList, Schema *schema, string relName) {
    this->myType = SELECTFILE;
    this->schema = schema;
    this->dbfilePath = "database/" + relName + ".bin";
    cnf.GrowFromParseTree(selectList, schema, literal);
}

void SelectFileNode :: run() {
    dbfile.Open(dbfilePath.c_str());
    selectFile.Run(dbfile, outpipe, cnf, literal);
}

void SelectFileNode :: print() {
    cout << endl << "Select File Operation Output pipe ID " << this->getPipeID() << endl;
    cout << endl << "Output Schema:" << endl;
    this->schema->Print();
    cout << endl << "Select File CNF:" << endl;
    /*if(this->cnf.getNumAnds() > 0){
        this->cnf.Print (this->schema, literal, 0);
    }else{
        cout<<"  No selection predicate";
    }*/
    this->cnf.Print();
}

SelectPipeNode :: SelectPipeNode(OpTreeNode *child, AndList *selectList) {
    this->myType = SELECTPIPE;
    this->left = child;
    this->schema = child->getSchema();
    cnf.GrowFromParseTree(selectList, schema, literal);
};

void SelectPipeNode :: run() {
    selectPipe.Run(left->outpipe, outpipe, cnf, literal);
}

void SelectPipeNode :: print() {
    cout << endl << "Select Pipe Operation Input pipe ID " << this->left->getPipeID() << " Output pipe ID " << this->getPipeID() << endl;
    cout << endl << "Output Schema:" << endl;
    this->schema->Print();
    cout << endl << "Select Pipe CNF:" << endl;
    /* if(this->cnf.getNumAnds() > 0){
         this->cnf.Print (this->schema, literal, 0);
     }else{
         cout<<"  No selection predicate";
     }*/
    this->cnf.Print();
};


JoinNode :: JoinNode(OpTreeNode *leftChild, OpTreeNode *rightChild, AndList *joinList) {
    this->myType = JOIN;
    this->left = leftChild;
    this->right = rightChild;
    // this->offset = offset;
    joinSchema();
    this->cnf.GrowFromParseTree(joinList, left->getSchema(), right->getSchema(), literal);
};

void JoinNode :: run() {
    join.Run(left->outpipe, right->outpipe, outpipe, cnf, literal);
}

void JoinNode :: joinSchema()
{
    int resNumAttrs = left->getSchema()->GetNumAtts() + right->getSchema()->GetNumAtts();
    Attribute *jointAtts = new Attribute[resNumAttrs];

    for (int i = 0; i < left->getSchema()->GetNumAtts(); i++) {
        jointAtts[i].name = left->getSchema()->GetAtts()[i].name;
        jointAtts[i].myType = left->getSchema()->GetAtts()[i].myType;
    }

    for (int j = 0; j < right->getSchema()->GetNumAtts(); j++) {
        jointAtts[j + left->getSchema()->GetNumAtts()].name = right->getSchema()->GetAtts()[j].name;
        jointAtts[j + left->getSchema()->GetNumAtts()].myType = right->getSchema()->GetAtts()[j].myType;
    }

    schema = new Schema("joined", resNumAttrs, jointAtts);
};

void JoinNode :: print() {
    cout << endl << "Join Operation Input pipe ID " << this->left->getPipeID() << " Input pipe ID " << this->right->getPipeID() << " Output pipe ID " << this->getPipeID() << endl;
    cout << endl << "Output Schema:" << endl;
    this->schema->Print();
    /*if(this->cnf.getNumAnds()>0) {
        cout << "\nJoin CNF : " << endl;
        this->cnf.Print(this->schema, literal, offset);
    }*/
    cout << endl << "Join CNF:" << endl;
    this->cnf.Print();
};

DuplicateRemovalNode :: DuplicateRemovalNode(OpTreeNode *child) {
    this->myType = DUPLICATEREMOVAL;
    this->left = child;
    this->schema = child->getSchema();
};

void DuplicateRemovalNode :: run() {
    duplicateRemoval.Run(left->outpipe, outpipe, *schema);
}

void DuplicateRemovalNode :: print() {
    cout << endl << "DuplicateRemoval Operation Input pipe ID " << this->left->getPipeID() << " Output pipe ID " << this->getPipeID() << endl;
    cout << endl << "Output Schema:" << endl;
    this->schema->Print();
};


GroupByNode :: GroupByNode(OpTreeNode *child, NameList *groupingAtts, FuncOperator *func) {
    this->myType = GROUPBY;
    this->left = child;
    this->groupByFunc = func;
    this->computeMe.GrowFromParseTree(func, *child->getSchema());
    getOrder(groupingAtts);
    groupBySchema();
};

void GroupByNode :: run() {
    groupBy.Run(left->outpipe, outpipe, groupOrder, computeMe);
}

void GroupByNode :: groupBySchema() {
    Attribute* atts = new Attribute[groupOrder.numAtts + 1];
    atts[0].name = "SUM";
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
    schema = new Schema("out_shema", groupOrder.numAtts + 1, atts);
}

void GroupByNode :: getOrder(NameList* groupingAtts) {
    Schema* inputSchema = left->getSchema();
    while (groupingAtts) {
        groupOrder.whichAtts[groupOrder.numAtts] = inputSchema->Find(groupingAtts->name);
        groupOrder.whichTypes[groupOrder.numAtts] = inputSchema->FindType(groupingAtts->name);
        ++groupOrder.numAtts;
        groupingAtts = groupingAtts->next;
    }
}

void GroupByNode :: print() {
    cout << endl << "GroupBy Operation Input pipe ID " << this->left->getPipeID() << " Output pipe ID " << this->getPipeID() << endl;
    cout << endl << "Output Schema:" << endl;
    this->schema->Print();
    cout << endl << "Corresponding OrderMaker:" << endl;
    groupOrder.Print();
    cout << endl << "Corresponding Function:" << endl;
    cout << joinFunc(this->groupByFunc) << endl;
};

string GroupByNode :: joinFunc(FuncOperator *func) {
    string str;
    if (func) {
        if (func->leftOperator) {
            str.append(joinFunc(func->leftOperator));
        }
        if (func->leftOperand) {
            str.append(func->leftOperand->value);
        }
        switch (func->code) {
            case 42:
                str.append(" * ");
                break;
            case 43:
                str.append(" + ");
                break;
            case 44:
                str.append(" / ");
                break;
            case 45:
                str.append(" - ");
                break;
        }
        if (func->right) {
            str.append(joinFunc(func->right));
        }
    }
    return str;
};


SumNode :: SumNode(OpTreeNode *child, FuncOperator *func) {
    this->myType = SUM;
    this->left = child;
    this->sumFunc = func;
    this->computeMe.GrowFromParseTree(func, *child->getSchema());
    sumSchema();
};

void SumNode :: run() {
    sum.Run(left->outpipe, outpipe, computeMe);
}

void SumNode :: print() {
    cout << endl << "Sum Operation Input pipe ID " << this->left->getPipeID() << " Output pipe ID " << this->getPipeID() << endl;
    cout << endl << "Output Schema:" << endl;
    this->schema->Print();
    cout << endl << "Corresponding Function:" << endl;
    cout << joinFunc(this->sumFunc) << endl;
};

void SumNode :: sumSchema() {
    Attribute *atts = new Attribute[1];
    atts[0].name = "SUM";
    if (computeMe.returnsInt == 0) {
        atts[0].myType = Double;
    }
    else {
        atts[0].myType = Int;
    }
    schema = new Schema("SUM", 1, atts);
}

string SumNode :: joinFunc(FuncOperator *func) {
    string str;
    if (func) {
        if (func->leftOperator) {
            str.append(joinFunc(func->leftOperator));
        }
        if (func->leftOperand) {
            str.append(func->leftOperand->value);
        }
        switch (func->code) {
            case 42:
                str.append("*");
                break;
            case 43:
                str.append("+");
                break;
            case 44:
                str.append("/");
                break;
            case 45:
                str.append("-");
                break;
        }
        if (func->right) {
            str.append(joinFunc(func->right));
        }
    }
    return str;
};

ProjectNode :: ProjectNode(OpTreeNode *child, NameList* attrsLeft) {
    this->myType = PROJECT;
    this->left = child;
    this->attsLeft = attrsLeft;
    this->schema = child->getSchema()->Project(attsLeft, attributesToSelect);
};

void ProjectNode :: run() {
    project.Run(left->outpipe, outpipe, attributesToSelect, this->left->getSchema()->GetNumAtts(), this->schema->GetNumAtts());
}

void ProjectNode :: print() {
    cout << endl << "Project Operation Input pipe ID " << this->left->getPipeID() << " Output pipe ID " << this->getPipeID() << endl;
    cout << endl << "Output Schema:" << endl;
    this->schema->Print();
    cout << endl << "Attributes to Keep:" << endl;
    NameList* nameList = attsLeft;
    while (nameList)
    {
        cout << nameList->name << " ";
        nameList = nameList->next;
    }
    cout << endl;
};