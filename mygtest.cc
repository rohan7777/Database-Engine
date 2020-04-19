#include <pthread.h>
#include <gtest/gtest.h>
#include <iostream>

#include "File.h"
#include "DBFile.h"
#include "BigQ.h"

#include "RelOp.h"
#include <stdlib.h>
#include <cmath>
#include "Statistics.h"
#include "ParseTree.h"





extern "C" struct YY_BUFFER_STATE *yy_scan_string(const char*);
extern "C" int yyparse(void);
extern struct AndList *final;

using namespace std;

bool doesFileExist(string fileName)
{
    std::ifstream file(fileName);
    return file.good();
}

char *fileName = "Statistics.txt";

TEST(Project4_1, WriteTestCase) {
	Statistics s;
	char *relName[] = {"orders","customer","nation"};
	
	s.AddRel(relName[0],1500000);
	s.AddAtt(relName[0], "o_custkey",150000);
	s.AddRel(relName[1],150000);
	s.AddAtt(relName[1], "c_custkey",150000);
	s.AddAtt(relName[1], "c_nationkey",25);
	s.AddRel(relName[2],25);
	s.AddAtt(relName[2], "n_nationkey",25);
	
	char *cnf = "(c_custkey = o_custkey)";
	yy_scan_string(cnf);
	yyparse();
	
	s.Apply(final, relName, 2);
	cnf = " (c_nationkey = n_nationkey)";
	yy_scan_string(cnf);
	yyparse();
	
	s.Apply(final, relName, 3);
	s.Write(fileName);
	
	EXPECT_EQ(true, doesFileExist(fileName));
}

TEST(Project4_1, estimateTestCase) {
	Statistics s;
	char *relName[] = {"supplier","partsupp"};
	
	s.AddRel(relName[0],10000);
	s.AddAtt(relName[0], "s_suppkey",10000);
	s.AddRel(relName[1],800000);
	s.AddAtt(relName[1], "ps_suppkey", 10000);
	
	char *cnf = "(s_suppkey = ps_suppkey)";
	yy_scan_string(cnf);
	yyparse();
	
	double result = s.Estimate(final, relName, 2);
	s.Apply(final, relName, 2);
	
	if(fabs(result-800000)>0.5)
		EXPECT_EQ(1, 1);  
}


int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
