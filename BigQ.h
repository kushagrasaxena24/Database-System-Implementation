#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Schema.h"
#include "Comparison.h"
#include "ComparisonEngine.h"


using namespace std;

class BigQ {
	File myFile;
public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
	static int split(Page *p, int value, OrderMaker *sort_order);
	static bool myfunction (int i,int j) { return (i<j); }
	static int indexing(void *);
	static void sort_run(Page*,int,File&,int&,OrderMaker *);
	static int sort_old(Page *p, int value, OrderMaker *sort_order, int a, int b);
	static int sort_new(Page *p, int value,OrderMaker *sort_order, int run, int flag);
	static void* Two_Pass_Merge_Sort2(void* arg,int check);
	~BigQ ();
};

class bqData{
public:
    Pipe *in;
    Pipe *out;
	File currentFile;
    OrderMaker *order;
    int runLength;
    File myFile;
};

class CompareR{
    ComparisonEngine myCE;
	OrderMaker* order;
public:
	CompareR(OrderMaker* sortorder) {order=sortorder;}

	bool operator()(Record *R1,Record *R2) {
		return myCE.Compare(R1,R2,order)<0;
	}
};
class RunRecord{
public:
	Record myRecord;
	int runNum;
};
class CompareQ{
    ComparisonEngine myCE;
	OrderMaker* order;
public:
	CompareQ(OrderMaker *sortorder){order=sortorder;}
	bool operator()(RunRecord *runR1,RunRecord *runR2)
	{
		if(myCE.Compare(&(runR1->myRecord),&(runR2->myRecord),order)>=0)
		return true;
		return false;
	}
};



#endif
