#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "BigQ.h"
#include "Function.h"
#include <sstream>
#include <stdlib.h>
#include <vector>
#include <string>
#include <stdlib.h>
#include "Pthreadutil.h"
#include "Defs.h"
#include <pthread.h>

class RelationalOp {
public:
  // blocks the caller until the particular relational operator
  // has run to completion
  virtual void WaitUntilDone ();

  // tell us how much internal memory the operation can use
  virtual void Use_n_Pages (int n) = 0;

protected:
  pthread_t worker;
  static int create_joinable_thread(pthread_t *thread,
                                    void *(*start_routine) (void *), void *arg);
};

class SelectFile : public RelationalOp {

	static void *exec(void *);
	static void MovePointer(DBFile*);

	private:
		pthread_t thread;  //the thread that run method will spawn
		int nPages;
		DBFile *inFile;
		Pipe *outPipe;
		CNF *selOp;
		Record *literal;


	public:
		static int a;
		void dofile();
		void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
		void WaitUntilDone ();
		void Use_n_Pages (int n);

};

class SelectPipe : public RelationalOp {	// streaming operation

	static void *pipeexec(void *);

	private:
		pthread_t thread;  //the thread that run method will spawn
		int nPages;
		Pipe *inPipe, *outPipe;
		CNF *selOp;
		Record *literal;
		void DoSelectPipe();

	public:
		void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
		void WaitUntilDone ();
		void Use_n_Pages (int n);

};

class Project : public RelationalOp {

	static void *projectexec(void *);
	static void shut(Pipe *);

	private:
		pthread_t thread;  //the thread that run method will spawn
		int nPages; //the buffer that the operator can use
		Pipe *inPipe, *outPipe;
		int *keepMe;
		int numAttsInput, numAttsOutput;


	public:

		void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) ;
		void WaitUntilDone () ;
		void Use_n_Pages (int n) ;
};

class DuplicateRemoval : public RelationalOp {



	private:
		pthread_t thread;  //the thread that run method will spawn
		int nPages; //the buffer that the operator can use
		Pipe *inPipe, *outPipe;
		//Pipe *sortPipe(100);
		Schema *mySchema;
		void DoDuplicateRemoval();
		void ClearSortedPipe(Pipe *P, OrderMaker *om);
	public:
		//int nPages;
		static void *duprem(void *);
		void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema) ;
		void WaitUntilDone () ;
		void Use_n_Pages (int n) ;
};



class SortBasedOp: public RelationalOp {
public:
  SortBasedOp(size_t runLen = 128): runLength(runLen) {}
  void Use_n_Pages (int n) { runLength = n; }  // the remaining page serves as input buffer

protected:
  size_t runLength;
};

class JoinBuffer;
class Join: public SortBasedOp {
public:
  void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	struct info_struct {

		Pipe *input;
		Pipe *output;
		OrderMaker *sort_order;
		int *run_length;

	}info;

private:
  MAKE_STRUCT6(Args, Pipe*, Pipe*, Pipe*, CNF*, Record*, size_t);
  static void* work(void* param);
  static void sortMergeJoin(Pipe* pleft, OrderMaker* orderLeft, Pipe* pright, OrderMaker* orderRight, Pipe* pout,
                             CNF* sel, Record* literal, size_t runLen);
  static void nestedLoopJoin(Pipe* pleft, Pipe* pright, Pipe* pout, CNF* sel, Record* literal, size_t runLen);
  static void joinBuf(JoinBuffer& buffer, DBFile& file, Pipe& out, Record& literal, CNF& sleOp);
  static void dumpFile(Pipe& in, DBFile& out);
	static int indexing(void *s);
};

class JoinBuffer {
  friend class Join;
  JoinBuffer(size_t npages);
  ~JoinBuffer();

  bool add (Record& addme);
  void clear () { size=nrecords=0; }

  size_t size, capacity;   // in bytes
  size_t nrecords;
  Record* buffer;
};

class Sum : public RelationalOp {

	private:
	pthread_t thread;
        Pipe *inPipe;
        Pipe *outPipe;
        Function *function;
	int nPages;

	public:
	void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
	Record* Makerec(Type , int , double );
	static void* dot(void*);
	void doSum();
	void WaitUntilDone () ;
	void Use_n_Pages (int n) ;
};

class GroupBy: public SortBasedOp {
public:
  void Run (Pipe& inPipe, Pipe& outPipe, OrderMaker& groupAtts, Function& computeMe);

private:
  MAKE_STRUCT5(Args, Pipe*, Pipe*, OrderMaker*, Function*, size_t);
  static void* work(void* param);
  template <class T>
  static void doGroup(Pipe* in, Pipe* out, OrderMaker* order, Function* func, size_t runLen);
  template <class T>
  static void putGroup(Record& cur, const T& sum, Pipe* out, OrderMaker* order) {
    cur.Project(order->getAtts(), order->getNumAtts(), cur.numAtts());
    cur.prepend(sum);
    out->Insert(&cur);
  }
};

class WriteOut : public RelationalOp {
	pthread_t thread;
	int nPages;
	public:
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) ;
	void WaitUntilDone ();
	void Use_n_Pages (int n) ;
};




/********************************************************************************
 * template definitions.                                                        *
 ********************************************************************************/
// template <class T>
// void Sum::doSum(Pipe* in, Pipe* out, Function* func) {
//   T sum=0; Record rec;
//   while (in->Remove(&rec))
//     sum += func->Apply<T>(rec);
//   Record result(sum);
//   out->Insert(&result);
// }

template <class T>   // similar to duplicate elimination
void GroupBy::doGroup(Pipe* in, Pipe* out, OrderMaker* order, Function* func, size_t runLen) {
  Pipe sorted(PIPE_SIZE);
  BigQ biq(*in, sorted, *order, (int)runLen);
  Record cur, next;
  ComparisonEngine cmp;

  // TODO: why not shutting down the input pipe here??
  if(sorted.Remove(&cur)) {  // cur holds the current group
    T sum = func->Apply<T>(cur);   // holds the sum for the current group
    while(sorted.Remove(&next))
      if(cmp.Compare(&cur, &next, order)) {
        putGroup(cur, sum, out, order);
        cur.Consume(&next);
        sum = func->Apply<T>(cur);
      } else sum += func->Apply<T>(next);
    putGroup(cur, sum, out, order);    // put the last group into the output pipeline
  }
}




#endif
