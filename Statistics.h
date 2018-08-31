#ifndef STATISTICS_
#define STATISTICS_

#include "ParseTree.h"
//#include <vector>
#include <map>
#include <string>

using namespace std;

typedef struct relInfo{
	map<string, int> attrs;
	int numTuples;
	int numRel;
} relInfo;

class Statistics{
private:
	map<string,relInfo> mymap;
	double tempRes;

public:
	Statistics();
	Statistics(Statistics &copyMe);	 // Performs deep copy
	~Statistics();


	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);
  static int copyrelatt(char *relName, int numTuples);

	char* SearchAttr(char * attrName);
	void Read(char *fromWhere);
  static void Read(char *fromWhere, bool flag);
	void Write(char *fromWhere);

	void Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);
   void Check (struct AndList *parseTree, char *relNames[], int numToJoin);
  double ApplyEstimate(struct AndList *parseTree, char **relNames, int numToJoin) { // apply and return
          double estimate = Estimate(parseTree, relNames, numToJoin);
          Apply(parseTree, relNames, numToJoin);
          return estimate;
        }
};

#endif
