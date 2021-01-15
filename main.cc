#include "Database.h"

using namespace std;

int main () {
    
    string choice;
    int dbState = 0; // 0 for shut down, 1 from fire up;
    
    Database database;
    
    while (true) {
        cout << endl << "Please select the operation:" << endl << endl;
        cout << "1) Create a new database (current database will be deleted)" << endl;
        cout << "2) Start current database" << endl;
        cout << "3) Execute a SQL query" << endl;
        cout << "4) Show tables in the database" << endl;
        cout << "5) Save current database state and shutdown the database" << endl;
        cout << "6) Save database state and exit program" << endl << endl;
        
        cout << "Your choice: ";
        cin >> choice;
        
        if (choice == "1") {
            if (dbState != 0) cerr << "Error: Current database must be shut down before creating a new one" << endl;
            else {
                database.Create();
                cout << "New database has been created successfully." << endl;
                cout << "********************************************************" << endl;
            }
        }
        else if (choice == "2") {
            if (dbState != 0) cerr << "Error: Database has already been fired up!" << endl;
            else {
                database.Open();
                dbState = 1;
                cout << "Database has been fired up." << endl;
                cout << "********************************************************" << endl;
            }
        }
        else if (choice == "3") {
            if (dbState != 1) cerr << "Error: Database hasn't been fired up yet!" << endl;
            else {
                database.Execute();
                cout << "********************************************************" << endl;
            }
        } 
        else if(choice == "4") {
            if (dbState != 1) cerr << "Error: Database hasn't been fired up yet!" << endl;
            else{
                database.showTables();
                cout << "********************************************************" << endl;
            }
        }
        else if (choice == "5") {
            if (dbState != 1) cerr << "Error: Database hasn't been fired up yet!" << endl;
            else {
                database.Close();
                dbState = 0;
                cout << "Database state has been saved." << endl;
                cout << "********************************************************" << endl;
            }
        }
        else if (choice == "6") {
                database.Close();
                cout << "Database state has been saved.\nShutting down the database\n~Good bye~" << endl;
                return 0;
        }
        else {
            cerr << "Invalid choice! Please select option between 1-6" << endl;
        }
    }
}


