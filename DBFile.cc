#include <fstream>
#include "DBFile.h"
#include "HeapFile.h"
#include "SortedFile.h"
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#define ERROR(msg...) {\
		printf(msg);\
		exit(-1);\
}

using std::string;
using std::ifstream;

DBFile::DBFile(): db(NULL) {

}

int DBFile::Create (char* fpath, fType ftype, void* startup) {
  if(db)
  ERROR("File already opened.");
   if(ftype==HEAP) 
      db = new HeapFile; 
    else if(ftype==SORTED)
     db = new SortedFile;
    else
     db = NULL;
  if(db==NULL)
  if(db==NULL)
  ERROR("Invalid file type.");
  return db->Create(fpath, startup);
}
string  DBFile::getMetaFile(char * fpath) {
  string tablename="";
  if(fpath!=NULL)
  {
      tablename=DBFileBase::getTableName(fpath);
  }
  return tablename+".meta";
}
int DBFile::Open (char* fpath) {
  if(db)
  ERROR("File already opened.");
  int ftype = HEAP;
  string metaFile = getMetaFile(fpath);
  ifstream ifs(metaFile.c_str());
  if (ifs) {
    ifs >> ftype;  
    ifs.close();
  }
   if(static_cast<fType>(ftype)==HEAP) 
      db = new HeapFile; 
    else if(static_cast<fType>(ftype)==SORTED)
     db = new SortedFile;
    else
     db = NULL;
  if(db==NULL)
  ERROR("Invalid file type.");
  return db->Open(fpath);
}

void DBFile::createFile(fType ftype) {
  switch (ftype) {
    case HEAP: db = new HeapFile; break;
    case SORTED: db = new SortedFile; break;
    default: db = NULL;
  }
  if(db==NULL)
    ERROR("Invalid file type.");
}

int DBFile::Close() {
  return db->Close();
}

void DBFile::Add (Record& addme) {
  return db->Add(addme);
}

void DBFile::Load (Schema& myschema, char* loadpath) {
  return db->Load(myschema, loadpath);
}

void DBFile::MoveFirst() {
  return db->MoveFirst();
}

int DBFile::GetNext (Record& fetchme) {
  return db->GetNext(fetchme);
}

int DBFile::GetNext (Record& fetchme, CNF& cnf, Record& literal) { 
  return db->GetNext(fetchme, cnf, literal);
}


DBFile::~DBFile() { delete db; }

int DBFileBase::Create(char* fpath, void* startup) {
  theFile.Open(0, fpath);
  return 1;
}

int DBFileBase::Open (char* fpath) {
  theFile.Open(1, fpath);
  return 1;
}

void DBFileBase::Load (Schema& myschema, char* loadpath) {
  startWrite();
  FILE* ifp = fopen(loadpath, "r");
  if(ifp==NULL)
  ERROR(loadpath);

  Record next;
  curPage.EmptyItOut();  // creates the first page
  while (next.SuckNextRecord(&myschema, ifp)) Add(next);
}

int DBFileBase::GetNext (Record& fetchme) {
  while (!curPage.GetFirst(&fetchme)) {
    if(++curPageIdx > theFile.lastIndex()) return 0;  // no more records
    theFile.GetPage(&curPage, curPageIdx);
  }
  return 1;
}
