// tests.cpp
#include <pthread.h>
#include <gtest/gtest.h>
//#include "Record.h"
/*#include "File.h"
#include "DBFile.h"
#include "BigQ.h"
#include "Pipe.cc"*/
#include "Database.h"


bool doesFileExist(string fileName)
{
    std::ifstream file(fileName);
    return file.good();
}

TEST(Project5, checkIfDBIsCreated) {

    Database db;
    db.Create();

    EXPECT_EQ(0, db.getTablesinDB().size());
    EXPECT_EQ(0, db.getSchemaInDB().size());
}

TEST(Project5, checkIfDBFileCreated) {

    Database db;
    db.Create();

    EXPECT_EQ(true, doesFileExist("database/Database"));
}


int main(int argc, char **argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
