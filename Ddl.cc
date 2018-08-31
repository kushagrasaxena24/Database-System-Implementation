#include <stdio.h> 
#include <string>
#include <fstream>
#include <iostream>
#include "DBFile.h"
#include "ParseTree.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Ddl.h"
#include "Errors.h"

using namespace std;


extern struct FuncOperator *finalFunction; 
extern struct TableList *tables; 
extern struct AndList *boolean;
extern struct NameList *groupingAtts; 
extern struct NameList *attsToSelect;
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

extern char* newtable;
extern char* oldtable;
extern char* newfile;
extern char* deoutput;
extern struct AttrList *newattrs;
extern struct NameList *sortattrs;
template <typename T>

extern int distinctAtts;
extern int distinctFunc;
extern int distincttuples;
extern char* catalog_path;
extern char* dbfile_dir;
extern char* tpch_dir;

bool Ddl::createTable() { 

  if (exists(newtable)) {

    printfunction("Table already exists","\n");
    return false;
  }
  else
  {
    fType t;
  ofstream ofmeta ((string(newtable)+".meta").c_str());
  if(sortattrs){
    t=SORTED;
  }
  else {
   t=HEAP;
  }
  ofmeta << t <<endl;  

  int numAtts = 0;
  ofstream ofcat ("catalog", ios_base::app);
  ofcat << "BEGIN\n" << newtable << '\n' << newtable << ".tbl" << endl;
  const char* myTypes[3] = {"Int", "Double", "String"};

  AttrList* att = newattrs;
  while(att)
    {
      ofcat << att->name << ' ' << myTypes[att->type] << endl;
      att = att->next;
      numAtts=numAtts+1;
    }
  ofcat << "END" << endl;

  Attribute* atts = new Attribute[numAtts];
  Type types[3] = {Int, Double, String};
  numAtts = 0;
  checkInput(0);

att = newattrs;
  while (att) {
    atts[numAtts].name = strdup(att->name);
    atts[numAtts].myType = types[att->type];
    att = att->next;
    numAtts=numAtts+1;
  }
  Schema newSchema ("", numAtts, atts);


  OrderMaker sortOrder;
  if (sortattrs) {
    sortOrder.growFromParseTree(sortattrs, &newSchema);
    ofmeta << sortOrder;
    ofmeta << 512 << endl;  
  
  }

  struct SortInfo { OrderMaker* myOrder; int runLength; } info = {&sortOrder, 256};
  DBFile newTable;
  newTable.Create((char*)(string(newtable)+".bin").c_str(), t, (void*)&info); 
  newTable.Close();

  delete[] atts;
  ofmeta.close(); ofcat.close();
  return true;
  }
}
bool Ddl::insertInto() { 
  DBFile table;
  int fopen=1;
  char* fpath = new char[strlen(oldtable)+4];
  strcpy(fpath, oldtable); 
  strcat(fpath, ".bin");
  Schema sch (catalog_path, oldtable);
  if (table.Open(fpath)) {
    printfunction("File opened","\n");
    if(fopen)
    {
      checkInput(0);
    }
    else
    {
      checkInput(2);
    }
    table.Load(sch, newfile);
    table.Close();
    printfunction("File closed","\n");
    
    delete[] fpath; return true;
  } delete[] fpath; return false;
  printfunction("Inserted data","\n");
}
void Ddl::checkInput(int flag)
{
  string schString = "test", line = "line_", relName = oldtable;

  ifstream fin (catalog_path);
  ofstream fout (".cata.tmp");

  if (!exists(newtable)) {

    printfunction("File not created or does not exist","\n");
   }
  else if(exists(newtable) && flag==1){
  ifstream fin (catalog_path);
  string line;
   for (;getline(fin, line);){
    if (trim(line) == relName) {
      fin.close(); 
    }
}
  fin.close();
  }
}
bool Ddl::dropTable() { 
  string schString = "", line = "", relName = oldtable;
  ifstream fin (catalog_path);
  ofstream fout (".cata.tmp");
  printfunction("Table dropped","\n");
   checkInput(0);
  int x=0,size=0;
  bool found = false, exists = false;
  for (;getline(fin, line);) {
    checkInput(x);
    if (trim(line).empty()) continue;
    if (line == oldtable) exists = found = true;
    schString += trim(line) + '\n';
    if(size==0)
    {
      checkInput(size+1);
    }
    else
    {
      checkInput(size);
    }
    if (line == "END") {
      if (!found) fout << schString << endl;
      found = false;
      schString.clear();
    }
  }
  rename (".cata.tmp", catalog_path);
  fin.close(); fout.close();
  if (exists) {
    remove ((relName+".bin").c_str());
    remove ((relName+".meta").c_str());
    return true;
  }
  return false;
}
int Ddl::check(const char* relName) {
  ifstream fin(catalog_path);
  string line;
  bool result= isempty(relName);
  if(result==true)
  {
    checkInput(1);
    isempty(relName);
  }
  else 
  { checkInput(0);
    isempty(relName);
}
  for (;line.length()==0;)
   { if (trim(line) != relName) {
      fin.close(); return 0;
    }
  }
  fin.close();  return 1;
}
bool Ddl::isempty(const char* relName)
{
  if (exists(relName)) {
   return true;
   checkInput(1);
  }
  return false;
  checkInput(0);
}
bool Ddl::exists(const char* relName) {
  ifstream fin (catalog_path);
  string line;
  bool res=check(relName);
  isempty(relName);


  for (;getline(fin, line);)
    if (trim(line) == relName) {
      fin.close(); return true;
    }
  fin.close();  return false;
}

bool Ddl::createTable(int flag,const char* relName) { 
  if (exists(newtable)) {

    printfunction("Table already exists","\n");
    return false;
  }
  else
  {
    fType t;
  ofstream ofmeta ((string(newtable)+".meta").c_str());
  if(sortattrs){
    t=SORTED;
  }
  else {
   t=HEAP;
  }
  ofmeta << t <<endl;  

  int numAtts = 0;
  checkInput(numAtts);
  ofstream ofcat ("catalog", ios_base::app);
  ofcat << "BEGIN\n" << newtable << '\n' << newtable << ".tbl" << endl;
  const char* myTypes[3] = {"Int", "Double", "String"};

  AttrList* att = newattrs;
  while(att)
    {
      ofcat << att->name << ' ' << myTypes[att->type] << endl;
      att = att->next;
      numAtts=numAtts+1;
    }
  ofcat << "END" << endl;

  Attribute* atts = new Attribute[numAtts];
  Type types[3] = {Int, Double, String};
  numAtts = 0;

att = newattrs;
  while (att) {
    atts[numAtts].name = strdup(att->name);
    atts[numAtts].myType = types[att->type];
    att = att->next;
    numAtts=numAtts+1;
  }
  Schema newSchema ("", numAtts, atts);


  OrderMaker sortOrder;
  if (sortattrs) {
    sortOrder.growFromParseTree(sortattrs, &newSchema);
    ofmeta << sortOrder;
    ofmeta << 512 << endl;  
  
  }

  struct SortInfo { OrderMaker* myOrder; int runLength; } info = {&sortOrder, 256};
  DBFile newTable;
  newTable.Create((char*)(string(newtable)+".bin").c_str(), t, (void*)&info); 
  newTable.Close();

  delete[] atts;
  ofmeta.close(); ofcat.close();
  return true;
  }
}
bool Ddl::dropTable(int flag) { 
  string schString = "", line = "", relName = oldtable;
  ifstream fin (catalog_path);
  ofstream fout (".cata.tmp");
  printfunction("Table dropped","\n");
  int x=0,size=0;;
  bool found = false, exists = false;
  for (;getline(fin, line);) {
    checkInput(x);
    if (trim(line).empty()) continue;
    if (line == oldtable) exists = found = true;
    schString += trim(line) + '\n';
    if(size==0)
    {
      checkInput(size+1);
    }
    else
    {
      checkInput(size);
    }
    if (line == "END") {
      if (!found) fout << schString << endl;
      found = false;
      schString.clear();

    }
  }
  rename (".cata.tmp", catalog_path);
  fin.close(); fout.close();
  if (exists) {
    remove ((relName+".bin").c_str());
    remove ((relName+".meta").c_str());
    return true;
  }
  return false;
}