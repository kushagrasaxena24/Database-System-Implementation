#ifndef TEST_H
#define TEST_H
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <math.h>
#include "Function.h"
#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
using namespace std;

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


const char *settings = "test.cat";


char *catalog_path, *dbfile_dir, *tpch_dir = NULL;

extern "C" {
	int yyparse(void);   
	int yyfuncparse(void);   
	void init_lexical_parser (char *); 
	void close_lexical_parser (); 
	void init_lexical_parser_func (char *); 
	void close_lexical_parser_func ();
}

extern struct AndList *final;
extern struct FuncOperator *finalfunc;
extern FILE *yyin;

typedef struct {
	Pipe *pipe;
	OrderMaker *order;
	bool print;
	bool write;
}testutil;

class relation {

private:
	char *rname;
	char *prefix;
	char rpath[100]; 
	Schema *rschema;
public:
	relation (char *_name, Schema *_schema, char *_prefix) :
		rname (_name), rschema (_schema), prefix (_prefix) {
		sprintf (rpath, "%s%s.bin", prefix, rname);
	}
	char* name () { return rname; }
	char* path () { return rpath; }
	Schema* schema () { return rschema;}
	void info () {

		printfunction(" relation info","\n");
		
	
		printfunction("\t"," name: ",name (),"\n");


		printfunction("\t"," path: ",path (),"\n");
		

		   
	}

	void get_cnf (char *input, CNF &cnf_pred, Record &literal) {
		init_lexical_parser (input);
  		if (yyparse() != 0) {

  			printfunction(" Error: can't parse your CNF.","\n");
			
			exit (1);
		}
		cnf_pred.GrowFromParseTree (final, schema (), literal); 
		close_lexical_parser ();
	}

	void get_cnf (char *input, Function &fn_pred) {
		init_lexical_parser_func (input);
  		if (yyfuncparse() != 0) {

  			printfunction(" Error: can't parse your CNF.","\n");
			
			exit (1);
		}
		fn_pred.GrowFromParseTree (finalfunc, *(schema ())); 
		close_lexical_parser_func ();
	}

	void get_cnf (CNF &cnf_pred, Record &literal) {

		printfunction("\n","Enter CNF predicate (when done press ctrl-D):","\n","\t");
		
  		if (yyparse() != 0) {

  			printfunction(" Error: can't parse your CNF.","\n");
			
			exit (1);
		}
		cnf_pred.GrowFromParseTree (final, schema (), literal); 
	}

	void get_file_cnf (const char *fpath, CNF &cnf_pred, Record &literal) {
		yyin = fopen (fpath, "r");
  		if (yyin == NULL) {


  			printfunction(" Error: can't open file ",fpath," for parsing","\n");
			
			exit (1);
		}
		if (yyparse() != 0) {
			printfunction(" Error: can't parse your CNF.","\n");
			
			exit (1);
		}
		cnf_pred.GrowFromParseTree (final, schema (), literal);
		
	}


	void get_sort_order (OrderMaker &sortorder) {

		printfunction("\n","specify sort ordering (when done press ctrl-D):","\n\t");
		
  		if (yyparse() != 0) {
  					printfunction(" Error: can't parse your CNF.","\n");
			
			exit (1);
		}
		Record literal;
		CNF sort_pred;
		sort_pred.GrowFromParseTree (final, schema (), literal); 
		OrderMaker dummy;
		sort_pred.GetSortOrders (sortorder, dummy);
	}
};

void get_cnf (char *input, Schema *left, CNF &cnf_pred, Record &literal) {
	init_lexical_parser (input);
  	if (yyparse() != 0) {
  		printfunction(" Error: can't parse your CNF ",input,"\n");
		
		exit (1);
	}
	cnf_pred.GrowFromParseTree (final, left, literal); 
	close_lexical_parser ();
}

void get_cnf (char *input, Schema *left, Schema *right, CNF &cnf_pred, Record &literal) {
	init_lexical_parser (input);
  	if (yyparse() != 0) {

  		printfunction(" Error: can't parse your CNF ",input,"\n");
		
		exit (1);
	}
	cnf_pred.GrowFromParseTree (final, left, right, literal); 
	close_lexical_parser ();
}

void get_cnf (char *input, Schema *left, Function &fn_pred) {
		init_lexical_parser_func (input);
  		if (yyfuncparse() != 0) {
  			printfunction(" Error: can't parse your arithmetic expr. ",input,"\n");
			
			exit (1);
		}
		fn_pred.GrowFromParseTree (finalfunc, *left); 
		close_lexical_parser_func ();
}

relation *rel;

char *supplier = "supplier"; 
char *partsupp = "partsupp"; 
char *part = "part"; 
char *nation = "nation"; 
char *customer = "customer"; 
char *orders = "orders"; 
char *region = "region"; 
char *lineitem = "lineitem"; 

relation *s, *p, *ps, *n, *li, *r, *o, *c;

void setup () {
	FILE *fp = fopen (settings, "r");
	if (fp) {
		char *mem = (char *) malloc (80 * 3);
		catalog_path = &mem[0];
		dbfile_dir = &mem[80];
		tpch_dir = &mem[160];
		char line[80];
		fgets (line, 80, fp);
		sscanf (line, "%s\n", catalog_path);
		fgets (line, 80, fp);
		sscanf (line, "%s\n", dbfile_dir);
		fgets (line, 80, fp);
		sscanf (line, "%s\n", tpch_dir);
		fclose (fp);
		if (! (catalog_path && dbfile_dir && tpch_dir)) {
			cerr << " Test settings file 'test.cat' not in correct format.\n";
			free (mem);
			exit (1);
		}
	}
	else {
		cerr << " Test settings files 'test.cat' missing \n";
		exit (1);
	}


	printfunction(" \n","** IMPORTANT: MAKE SURE THE INFORMATION BELOW IS CORRECT **","\n");
	printfunction("catalog location:","\t",catalog_path,"\n");
	
	printfunction(" tpch files dir: ","\t",tpch_dir,"\n");
	printfunction(" heap files dir: ","\t",dbfile_dir,"\n");
	
	printfunction("\n","\n")


	s = new relation (supplier, new Schema (catalog_path, supplier), dbfile_dir);
	p = new relation (part, new Schema (catalog_path, part), dbfile_dir);
	ps = new relation (partsupp, new Schema (catalog_path, partsupp), dbfile_dir);
	n = new relation (nation, new Schema (catalog_path, nation), dbfile_dir);
	li = new relation (lineitem, new Schema (catalog_path, lineitem), dbfile_dir);
	r = new relation (region, new Schema (catalog_path, region), dbfile_dir);
	o = new relation (orders, new Schema (catalog_path, orders), dbfile_dir);
	c = new relation (customer, new Schema (catalog_path, customer), dbfile_dir);
}

void cleanup () {
	delete s, p, ps, n, li, r, o, c;
	free (catalog_path);
}

#endif
