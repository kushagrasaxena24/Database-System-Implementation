#ifndef QUERY_PLAN_H_
#define QUERY_PLAN_H_

#include <iostream>
#include <string>
#include <vector>
#include "RelOp.h"
#include "Ddl.h"
#include "DBFile.h"
#include "Schema.h"
#include "Function.h"
#include "ParseTree.h"
#include "Statistics.h"
#include "Comparison.h"
using namespace std;
#define MAX_RELS 12
#define MAX_RELNAME 50
#define MAX_ATTS 100

class QueryNode;
class QueryPlan {
public:
  QueryPlan(Statistics* st);
  ~QueryPlan() { if (root) delete root; }

  void plan();
  void print(ostream& os = cout) const;
  void setOutput(char* out);
  void execute();

private:
  void makeLeafs();
  void makeJoins();
  void orderJoins();
  void makeSums();
  void makeProjects();
  void makeDistinct();
  void makeWrite();
  void checkDuplicate();
  int evalOrder(vector<QueryNode*> operands, Statistics st, int bestFound);

  QueryNode* root;
  vector<QueryNode*> nodes;
  string outName;
  FILE* outFile;

  Statistics* stat;
  AndList* used;  
  void recycleList(AndList* alist) { concatList(used, alist); }
  static void concatList(AndList*& left, AndList*& right);

  QueryPlan(const QueryPlan&);
  QueryPlan& operator=(const QueryPlan&);
};

class Pipe; class RelationalOp;
class QueryNode {
  friend class QueryPlan;
  friend class UnaryNode;
  friend class BinaryNode;
  friend class ProjectNode;
  friend class DedupNode;
  friend class JoinNode;
  friend class SumNode;
  friend class GroupByNode;
  friend class WriteNode;
public:
  virtual ~QueryNode();

protected:
  QueryNode(const string& op, Schema* out, Statistics* st);
  QueryNode(const string& op, Schema* out, char* rName, Statistics* st);
  QueryNode(const string& op, Schema* out, char* rNames[], size_t num, Statistics* st);

  virtual void print(ostream& os = cout, size_t level = 0) const;
  virtual void printOperator(ostream& os = cout, size_t level = 0) const;
  virtual void printSchema(ostream& os = cout, size_t level = 0) const;
  virtual void printAnnot(ostream& os = cout, size_t level = 0) const = 0;
  virtual void printPipe(ostream& os, size_t level = 0) const = 0;
  virtual void printChildren(ostream& os, size_t level = 0) const = 0;

  virtual void execute(Pipe** pipes, RelationalOp** relops) = 0;

  static AndList* pushSelection(AndList*& alist, Schema* target);
  static bool containedIn(OrList* ors, Schema* target);
  static bool containedIn(ComparisonOp* cmp, Schema* target);

  string opName;
  Schema* outSchema;
  char* relNames[MAX_RELS];
  size_t numRels;
  int estimate, cost; 
  Statistics* stat;
  int pout; 
  static int pipeId;
};

class LeafNode: private QueryNode {  
  friend class QueryPlan;
  LeafNode (AndList*& boolean, AndList*& pushed,
            char* relName, char* alias, Statistics* st);
  ~LeafNode() { if (opened) dbf.Close(); }
  void printOperator(ostream& os = cout, size_t level = 0) const;
  void printAnnot(ostream& os = cout, size_t level = 0) const;
  void printPipe(ostream& os, size_t level) const;
  void printChildren(ostream& os, size_t level) const {}

  void execute(Pipe** pipes, RelationalOp** relops);

  DBFile dbf;
  bool opened;
  CNF selOp;
  Record literal;
};

class UnaryNode: protected QueryNode {
  friend class QueryPlan;
protected:
  UnaryNode(const string& opName, Schema* out, QueryNode* c, Statistics* st);
  virtual ~UnaryNode() { delete child; }
  void printPipe(ostream& os, size_t level) const;
  void printChildren(ostream& os, size_t level) const { child->print(os, level+1); }
  QueryNode* child;
  int pin; 
};

class BinaryNode: protected QueryNode { 
  friend class QueryPlan;
protected:
  BinaryNode(const string& opName, QueryNode* l, QueryNode* r, Statistics* st);
  virtual ~BinaryNode() { delete left; delete right; }
  void printPipe(ostream& os, size_t level) const;
  void printChildren(ostream& os, size_t level) const
  { left->print(os, level+1); right->print(os, level+1); }
  QueryNode* left;
  QueryNode* right;
  int pleft, pright; 
};

class ProjectNode: private UnaryNode {
  friend class QueryPlan;
  ProjectNode(NameList* atts, QueryNode* c);
  void printAnnot(ostream& os = cout, size_t level = 0) const;
  void execute(Pipe** pipes, RelationalOp** relops);
  int keepMe[MAX_ATTS];
  int numAttsIn, numAttsOut;
};

class DedupNode: private UnaryNode {
  friend class QueryPlan;
  DedupNode(QueryNode* c);
  void printAnnot(ostream& os = cout, size_t level = 0) const {}
  void execute(Pipe** pipes, RelationalOp** relops);
  OrderMaker dedupOrder;
};

class SumNode: private UnaryNode {
  friend class QueryPlan;
  SumNode(FuncOperator* parseTree, QueryNode* c);
  Schema* resultSchema(FuncOperator* parseTree, QueryNode* c);
  void printAnnot(ostream& os = cout, size_t level = 0) const;
  void execute(Pipe** pipes, RelationalOp** relops);
  Function f;
};

class GroupByNode: private UnaryNode {
  friend class QueryPlan;
  GroupByNode(NameList* gAtts, FuncOperator* parseTree, QueryNode* c);
  Schema* resultSchema(NameList* gAtts, FuncOperator* parseTree, QueryNode* c);
  void printAnnot(ostream& os = cout, size_t level = 0) const;
  void execute(Pipe** pipes, RelationalOp** relops);
  OrderMaker grpOrder;
  Function f;
};

class JoinNode: private BinaryNode {
  friend class QueryPlan;
  JoinNode(AndList*& boolean, AndList*& pushed, QueryNode* l, QueryNode* r, Statistics* st);
  void printAnnot(ostream& os = cout, size_t level = 0) const;
  void execute(Pipe** pipes, RelationalOp** relops);
  CNF selOp;
  Record literal;
};

class WriteNode: private UnaryNode {
  friend class QueryPlan;
  WriteNode(FILE*& out, QueryNode* c);
  void printAnnot(ostream& os = cout, size_t level = 0) const;
  void execute(Pipe** pipes, RelationalOp** relops);
  FILE*& outFile;
};

#endif

