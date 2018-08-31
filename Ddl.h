#ifndef _DDL_H_
#define _DDL_H_
#include<stdio.h>
#include<iostream>
#include<string.h>
#include <algorithm>

class Ddl {
public:
  bool createTable();
  bool createTable(int flag,const char* relName);

  bool insertInto();
  bool dropTable();
  bool dropTable(int flag);
  void checkInput(int f);
  void setOutput();
 
  

private:
  bool exists(const char* relName);
  bool isempty(const char* relName);

  int check(const char* relName);

  static inline std::string &ltrim(std::string &s) {
    s.erase(s.begin(), std::find_if(s.begin(), s.end(), std::not1(std::ptr_fun<int, int>(std::isspace))));
    return s;
  }

  static inline std::string &rtrim(std::string &s) {
    s.erase(std::find_if(s.rbegin(), s.rend(), std::not1(std::ptr_fun<int, int>(std::isspace))).base(), s.end());
    return s;
  }

  static inline std::string &trim(std::string &s) {
    std::string v=rtrim(s);
    return ltrim(v);
  }
};
#endif
