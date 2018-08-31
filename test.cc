#include <iostream>
#include "ParsedData.h"

using namespace std;

char* catalog_path = "catalog";
char* dbfile_dir = "../../bin/dbfiles/";
char* tpch_dir = "../../1G/";

int main (int argc, char* argv[]) {
  ParsedData exec;
  exec.run();
  return 0;
}
