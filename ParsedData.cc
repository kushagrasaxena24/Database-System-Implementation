#include <stdio.h> 
#include <string>
#include <fstream>
#include <iostream>
#include "Statistics.h"
#include "QueryPlan.h"
#include "Ddl.h"
#include "Errors.h"
#include "ParsedData.h"
#include "ParseTree.h"


using std::cout;
template <typename T>
void printfunction(T t)
{
  cout << t ;
}
template<typename T, typename... Args>
void printfunction(T t, Args... args)
{
  cout << t ;
  printfunction(args...) ;
}
// yyparese method is defined in y.tab.c
extern "C" {
  int yyparse(void);
}

extern char* oldtable;
extern struct TableList *tables;
extern int distinctAtts;
extern struct AndList *boolean;
extern int distinctFunc;
extern struct NameList *groupingAtts;
extern char* newtable;
extern struct NameList *attsToSelect;
extern char* deoutput;
extern struct FuncOperator *finalFunction;
extern char* newfile;
extern struct AttrList *newattrs; 
void ParsedData::run() {
    char *fileName = "Statistics.txt";
    Statistics s;
    Ddl d;
    QueryPlan plan(&s);
    bool flag=true;

while(true) {
    cout<<endl<<"*************Enter SQL Query****************"<<endl;
    if (yyparse() != 0) {
      cout<<"Cannot parse your CNF."<<endl;
      cout<<endl;
      continue;
    }
  
   s.Read(fileName);
   int newtab=0;
   int outputde=0;

    if(newtable)
    {
      newtab=1;
    }
    switch(newtab){
      case 1:
      if (d.createTable()) cout << "Create table " << newtable << endl;
      else
        cout<<"Table "<<newtable<<" already exists."<<endl;


      cout << endl;
      break;

      case 0: 
      switch(oldtable && newfile) {

      case 1:
      if (d.insertInto()) cout << "Insert into " << oldtable << endl;
      else cout << "Insert failed." << endl;
      break;

      case 0:
      switch(oldtable && !newfile) {
      case 1:
      if (d.dropTable()) cout << "Drop table " << oldtable << endl;
      else cout << "Table " << oldtable << " does not exist." << endl;
      break;

      case 0:
      if(deoutput)
      {
        outputde=1;
      }
      switch(outputde){
      case 1:  plan.setOutput(deoutput);
      break;
      case 0: 
      if (attsToSelect || finalFunction) {
      plan.plan();
      cout<<endl;
      plan.print();
      cout<<"Executing Query..."<<endl;
      plan.execute();
          }
       }
    }
 }
}
      clear(1);
  }

}
// TODO: free lists
void ParsedData::clear(int count) {
  while(count!=0)
  {
  newattrs = NULL;
  groupingAtts = NULL;
  attsToSelect = NULL;
  newtable = oldtable = newfile = deoutput = NULL;
  distinctAtts = distinctFunc = 0;
  finalFunction = NULL;
  tables = NULL;
  boolean = NULL;
  FATALIF (!remove ("*.tmp"), "remove tmp files failed");
  --count;
  }
}
