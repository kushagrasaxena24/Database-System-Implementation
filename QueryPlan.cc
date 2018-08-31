#include <cstring>
#include <climits>
#include <string>
#include <algorithm>
#include "Defs.h"
#include "Errors.h"
#include "Stl.h"
#include "QueryPlan.h"
#include "Pipe.h"
#include "RelOp.h"
#include "Ddl.h"
#include "HeapFile.h"

using namespace std;

#define popVector(vel, el1, el2)                \
  QueryNode* el1 = vel.back();                  \
  vel.pop_back();                               \
  QueryNode* el2 = vel.back();                  \
  vel.pop_back();

#define makeNode(pushed, recycler, nodeType, newNode, params)           \
  AndList* pushed;                                                      \
  nodeType* newNode = new nodeType params;                              \
  concatList(recycler, pushed);

#define freeAll(freeList)                                        \
  for (size_t __ii = 0; __ii < freeList.size(); ++__ii) {        \
    --freeList[__ii]->pipeId; free(freeList[__ii]);  } 

#define makeAttr(newAttr, name1, type1)                 \
  newAttr.name = name1; newAttr.myType = type1;

using std::endl;
using std::string;

extern char* catalog_path;
extern char* dbfile_dir;
extern char* tpch_dir;

extern FuncOperator* finalFunction;
extern TableList* tables;
extern AndList* boolean;
extern NameList* groupingAtts;
extern NameList* attsToSelect;
extern int distinctAtts;
extern int distinctFunc;



QueryPlan::QueryPlan(Statistics* st): root(NULL), outName("STDOUT"), stat(st), used(NULL) {}

void QueryPlan::plan() {

  makeLeafs(); 
  makeJoins();
  makeSums();
  makeProjects();
  checkDuplicate();
  makeDistinct();
  makeWrite();
  
  
  swap(boolean, used);
  FATALIF(used, "WHERE clause syntax error.");
}
void QueryPlan::checkDuplicate(){
  if(distinctAtts==0)
  {
    cout<<"No distinct attributes"<<endl;
  }
  else  {
    cout<<"Duplicacy may be detected"<<endl;
  }

}
void QueryPlan::print(ostream& os) const {
  root->print(os);
}

void QueryPlan::setOutput(char* out) {
  if(out)
  outName = out;
  else
  cout<<"Output empty"<<endl;
  outName=out;
}

void QueryPlan::execute() {
  int i=0;
  outFile = (outName == "STDOUT" ? stdout
    : outName == "NONE" ? NULL
    : fopen(outName.c_str(), "w"));  
    bool t=false;
    if(outFile)
    {
      t=true;
    }
    else{
      t=false;
    }
  if (t) {
    int numNodes = root->pipeId;
    Pipe** pipes = new Pipe*[numNodes];
    RelationalOp** relops = new RelationalOp*[numNodes];

    root->execute(pipes, relops);
    i=0;
    while(i<numNodes) {
      relops[i] -> WaitUntilDone();
      i++;
    }
   i=0;
   while(i<numNodes) {
      delete pipes[i]; delete relops[i];
      i++;
    }
    delete[] pipes; delete[] relops;
    if (outFile==stdout){
    
    } else {
      fclose(outFile);
    }
  }
  root->pipeId = 0;
  delete root; root = NULL;
  nodes.clear();
}


void QueryPlan::makeLeafs() {

  for (TableList* table = tables; table; table = table->next) {
    stat->CopyRel(table->tableName, table->aliasAs);
    makeNode(pushed, used, LeafNode, newLeaf, (boolean, pushed, table->tableName, table->aliasAs, stat));
    nodes.push_back(newLeaf);
  }

}

void QueryPlan::makeJoins() {
  orderJoins();
  while (nodes.size()>1) {
    popVector(nodes, left, right);
    makeNode(pushed, used, JoinNode, newJoinNode, (boolean, pushed, left, right, stat));
    nodes.push_back(newJoinNode);
  }
  root = nodes.front();
}

void QueryPlan::makeSums() {
  if (groupingAtts) {
    FATALIF (!finalFunction, "Grouping without aggregation functions!");
    FATALIF (distinctAtts, "No dedup after aggregate!");
    if (distinctFunc) root = new DedupNode(root);
    root = new GroupByNode(groupingAtts, finalFunction, root);
  } else if (finalFunction) {
    root = new SumNode(finalFunction, root);
  }
}

void QueryPlan::makeProjects() {
  if (attsToSelect && !finalFunction && !groupingAtts) root = new ProjectNode(attsToSelect, root);
}

void QueryPlan::makeDistinct() {
  if (distinctAtts) root = new DedupNode(root);
}

void QueryPlan::makeWrite() {
  root = new WriteNode(outFile, root);
}

void QueryPlan::orderJoins() {
  vector<QueryNode*> operands(nodes);
  sort(operands.begin(), operands.end());
  int minCost = INT_MAX, cost;
  do {           
    if ((cost=evalOrder(operands, *stat, minCost))<minCost && cost>0) {
      minCost = cost; nodes = operands;
    }
  } while (next_permutation(operands.begin(), operands.end()));
}

int QueryPlan::evalOrder(vector<QueryNode*> operands, Statistics st, int bestFound) {  
  vector<JoinNode*> freeList; 
  AndList* recycler = NULL;       
  while (operands.size()>1) {      
    popVector(operands, left, right);
    makeNode(pushed, recycler, JoinNode, newJoinNode, (boolean, pushed, left, right, &st));
    operands.push_back(newJoinNode);
    freeList.push_back(newJoinNode);
    if (newJoinNode->estimate<=0 || newJoinNode->cost>bestFound) break;  
  }
  int cost = operands.back()->cost;
  freeAll(freeList);
  concatList(boolean, recycler);   
  return operands.back()->estimate<0 ? -1 : cost;
}

void QueryPlan::concatList(AndList*& left, AndList*& right) {
  if (!left) { swap(left, right); return; }
  AndList *pre = left, *cur = left->rightAnd;
  for (; cur; pre = cur, cur = cur->rightAnd);
  pre->rightAnd = right;
  right = NULL;
}

int QueryNode::pipeId = 0;

QueryNode::QueryNode(const string& op, Schema* out, Statistics* st):
  opName(op), outSchema(out), numRels(0), estimate(0), cost(0), stat(st), pout(pipeId++) {}

QueryNode::QueryNode(const string& op, Schema* out, char* rName, Statistics* st):
  opName(op), outSchema(out), numRels(0), estimate(0), cost(0), stat(st), pout(pipeId++) {
  if (rName) relNames[numRels++] = strdup(rName);
}

QueryNode::QueryNode(const string& op, Schema* out, char* rNames[], size_t num, Statistics* st):
  opName(op), outSchema(out), numRels(0), estimate(0), cost(0), stat(st), pout(pipeId++) {
  for (; numRels<num; ++numRels)
    relNames[numRels] = strdup(rNames[numRels]);
}

QueryNode::~QueryNode() {
  delete outSchema;
  for (size_t i=0; i<numRels; ++i)
    delete[] relNames[i];
}

AndList* QueryNode::pushSelection(AndList*& alist, Schema* target) {
  AndList header; header.rightAnd = alist; 
  AndList *cur = alist, *pre = &header, *result = NULL;
  for (; cur; cur = pre->rightAnd)
    if (containedIn(cur->left, target)) {  
      pre->rightAnd = cur->rightAnd;
      cur->rightAnd = result;     
      result = cur;      
    } else pre = cur;
  alist = header.rightAnd; 
  return result;
}

bool QueryNode::containedIn(OrList* ors, Schema* target) {
  for (; ors; ors=ors->rightOr)
    if (!containedIn(ors->left, target)) return false;
  return true;
}

bool QueryNode::containedIn(ComparisonOp* cmp, Schema* target) {
  Operand *left = cmp->left, *right = cmp->right;
  return (left->code!=NAME || target->Find(left->value)!=-1) &&
         (right->code!=NAME || target->Find(right->value)!=-1);
}

LeafNode::LeafNode(AndList*& boolean, AndList*& pushed, char* relName, char* alias, Statistics* st):
  QueryNode("Select File", new Schema(catalog_path, relName, alias), relName, st), opened(false) {
  pushed = pushSelection(boolean, outSchema);
  estimate = stat->ApplyEstimate(pushed, relNames, numRels);
  selOp.GrowFromParseTree(pushed, outSchema, literal);
}

UnaryNode::UnaryNode(const string& opName, Schema* out, QueryNode* c, Statistics* st):
  QueryNode (opName, out, c->relNames, c->numRels, st), child(c), pin(c->pout) {}

BinaryNode::BinaryNode(const string& opName, QueryNode* l, QueryNode* r, Statistics* st):
  QueryNode (opName, new Schema(*l->outSchema, *r->outSchema), st),
  left(l), right(r), pleft(left->pout), pright(right->pout) {
  for (size_t i=0; i<l->numRels;)
    relNames[numRels++] = strdup(l->relNames[i++]);
  for (size_t j=0; j<r->numRels;)
    relNames[numRels++] = strdup(r->relNames[j++]);
}

ProjectNode::ProjectNode(NameList* atts, QueryNode* c):
  UnaryNode("Project", NULL, c, NULL), numAttsIn(c->outSchema->GetNumAtts()), numAttsOut(0) {
  Schema* cSchema = c->outSchema;
  Attribute resultAtts[MAX_ATTS];
  FATALIF (cSchema->GetNumAtts()>MAX_ATTS, "Too many attributes.");
  for (; atts; atts=atts->next, numAttsOut++) {
    FATALIF ((keepMe[numAttsOut]=cSchema->Find(atts->name))==-1,
             "Projecting non-existing attribute.");
    makeAttr(resultAtts[numAttsOut], atts->name, cSchema->FindType(atts->name));
  }
  outSchema = new Schema ("", numAttsOut, resultAtts);
}

DedupNode::DedupNode(QueryNode* c):
  UnaryNode("Deduplication", new Schema(*c->outSchema), c, NULL), dedupOrder(c->outSchema) {}

JoinNode::JoinNode(AndList*& boolean, AndList*& pushed, QueryNode* l, QueryNode* r, Statistics* st):
  BinaryNode("Join", l, r, st) {
  pushed = pushSelection(boolean, outSchema);
  estimate = stat->ApplyEstimate(pushed, relNames, numRels);
  cost = l->cost + estimate + r->cost;
  selOp.GrowFromParseTree(pushed, l->outSchema, r->outSchema, literal);
}

SumNode::SumNode(FuncOperator* parseTree, QueryNode* c):
  UnaryNode("Sum", resultSchema(parseTree, c), c, NULL) {
  f.GrowFromParseTree (parseTree, *c->outSchema);
}

Schema* SumNode::resultSchema(FuncOperator* parseTree, QueryNode* c) {
  Function fun;
  Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
  fun.GrowFromParseTree (parseTree, *c->outSchema);
  return new Schema ("", 1, atts[fun.resultType()]);
}

GroupByNode::GroupByNode(NameList* gAtts, FuncOperator* parseTree, QueryNode* c):
  UnaryNode("Group by", resultSchema(gAtts, parseTree, c), c, NULL) {
  grpOrder.growFromParseTree(gAtts, c->outSchema);
  f.GrowFromParseTree (parseTree, *c->outSchema);
}

Schema* GroupByNode::resultSchema(NameList* gAtts, FuncOperator* parseTree, QueryNode* c) {
  Function fun;
  Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
  Schema* cSchema = c->outSchema;
  fun.GrowFromParseTree (parseTree, *cSchema);
  Attribute resultAtts[MAX_ATTS];
  FATALIF (1+cSchema->GetNumAtts()>MAX_ATTS, "Too many attributes.");
  makeAttr(resultAtts[0], "sum", fun.resultType());
  int numAtts = 1;
  for (; gAtts; gAtts=gAtts->next, numAtts++) {
    FATALIF (cSchema->Find(gAtts->name)==-1, "Grouping by non-existing attribute.");
    makeAttr(resultAtts[numAtts], gAtts->name, cSchema->FindType(gAtts->name));
  }
  return new Schema ("", numAtts, resultAtts);
}

WriteNode::WriteNode(FILE*& out, QueryNode* c):
  UnaryNode("WriteOut", new Schema(*c->outSchema), c, NULL), outFile(out) {}


void LeafNode::execute(Pipe** pipes, RelationalOp** relops) {
  string dbName = string(relNames[0]) + ".bin";
  dbf.Open((char*)dbName.c_str()); opened = true;
  SelectFile* sf = new SelectFile();
  pipes[pout] = new Pipe(PIPE_SIZE);
  relops[pout] = sf;
  sf -> Run(dbf, *pipes[pout], selOp, literal);
}

void ProjectNode::execute(Pipe** pipes, RelationalOp** relops) {
  child -> execute(pipes, relops);
  Project* p = new Project();
  pipes[pout] = new Pipe(PIPE_SIZE);
  relops[pout] = p;
  p->Run(*pipes[pin], *pipes[pout], keepMe, numAttsIn, numAttsOut);
}

void DedupNode::execute(Pipe** pipes, RelationalOp** relops) {
  child -> execute(pipes, relops);
  DuplicateRemoval* dedup = new DuplicateRemoval();
  pipes[pout] = new Pipe(PIPE_SIZE);
  relops[pout] = dedup;
  dedup -> Run(*pipes[pin], *pipes[pout], *outSchema);
}

void SumNode::execute(Pipe** pipes, RelationalOp** relops) {
  child -> execute(pipes, relops);
  Sum* s = new Sum();
  pipes[pout] = new Pipe(PIPE_SIZE);
  relops[pout] = s;
  s -> Run(*pipes[pin], *pipes[pout], f);
}

void GroupByNode::execute(Pipe** pipes, RelationalOp** relops) {
  child -> execute(pipes, relops);
  GroupBy* grp = new GroupBy();
  pipes[pout] = new Pipe(PIPE_SIZE);
  relops[pout] = grp;
  grp -> Run(*pipes[pin], *pipes[pout], grpOrder, f);
}

void JoinNode::execute(Pipe** pipes, RelationalOp** relops) {
  left -> execute(pipes, relops); right -> execute(pipes, relops);
  Join* j = new Join();
  pipes[pout] = new Pipe(PIPE_SIZE);
  relops[pout] = j;
  j -> Run(*pipes[pleft], *pipes[pright], *pipes[pout], selOp, literal);
}

void WriteNode::execute(Pipe** pipes, RelationalOp** relops) {
  child -> execute(pipes, relops);
  WriteOut* w = new WriteOut();
  pipes[pout] = new Pipe(PIPE_SIZE);
  relops[pout] = w;
  w -> Run(*pipes[pin], outFile, *outSchema);
}

void QueryNode::print(ostream& os, size_t level) const {
  printOperator(os, level);
  printAnnot(os, level);
  printSchema(os, level);
  printPipe(os, level);
  printChildren(os, level);
}

void WriteNode::printAnnot(ostream& os, size_t level) const {
  os << "Output to " << outFile << endl;
}

void QueryNode::printOperator(ostream& os, size_t level) const {
  os  << opName << ": ";
}

void QueryNode::printSchema(ostream& os, size_t level) const {
#ifdef _OUTPUT_SCHEMA__
  os << "Output schema:" << endl;
  outSchema->print(os);
#endif
}

void LeafNode::printPipe(ostream& os, size_t level) const {
  os << "Output pipe: " << pout << endl;
}

void UnaryNode::printPipe(ostream& os, size_t level) const {
  os << "Output pipe: " << pout << endl;
  os << "Input pipe: " << pin << endl;
}

void BinaryNode::printPipe(ostream& os, size_t level) const {
  os << "Output pipe: " << pout << endl;
  os << "Input pipe: " << pleft << ", " << pright << endl;
}

void LeafNode::printOperator(ostream& os, size_t level) const {
  os  << "Select from " << relNames[0] << ": ";
}

void LeafNode::printAnnot(ostream& os, size_t level) const {
  selOp.Print();
}

void ProjectNode::printAnnot(ostream& os, size_t level) const {
  os << keepMe[0];
  for (size_t i=1; i<numAttsOut; ++i) os << ',' << keepMe[i];
  os << endl;
  os << numAttsIn << " input attributes; " << numAttsOut << " output attributes" << endl;
}

void JoinNode::printAnnot(ostream& os, size_t level) const {
  selOp.Print();
  os << "Estimate = " << estimate << ", Cost = " << cost << endl;
}

void SumNode::printAnnot(ostream& os, size_t level) const {
  os << "Function: "; (const_cast<Function*>(&f))->Print();
}

void GroupByNode::printAnnot(ostream& os, size_t level) const {
  os << "OrderMaker: "; (const_cast<OrderMaker*>(&grpOrder))->Print();
  os << "Function: "; (const_cast<Function*>(&f))->Print();
}



