#include "RelOp.h"

#include <stdio.h>
#include <iostream>
#include <cstdlib>


#include "Record.h"
#include "Errors.h"
#include "HeapFile.h"

#define FOREACH_INPIPE(rec, in)                 \
  Record rec;                                   \
  while (in->Remove(&rec))

#define FOREACH_INFILE(rec, f)                  \
  f.MoveFirst();                                \
  Record rec;                                   \
  while (f.GetNext(rec))

#ifndef END_FOREACH
#define END_FOREACH }
#endif

int SelectFile::a=0;

typedef struct params{

	Pipe *inPipe;
	Pipe *outPipe;
	CNF *selOp;
	DBFile *inFile;
	Schema *mySchema;
	Record *literal;
	int *keepMe;

	int numAttsInput;
	int numAttsOutput;

} params;

//int SelectFile::a;


void *SelectPipe::pipeexec(void *arg) {

	cout<<"created thread in Select Pipe, now running"<<endl;


	params *sp = (params *) arg;


	int ct=0;
	int totalrecs=0;


	Record *r = new Record();
	ComparisonEngine eng;

	Pipe *input=sp->inPipe;

	if(input->Remove(r)!=1)
	{
		cout<<" \n the remove failed, there must be an error in the pipe sent check !!!";
		exit(0);
	}

	do{

		++totalrecs;

		if(ct>=99)
		{
			// cout<<"\nOutput pipe limit reached, exiting ...";
			// sp->outPipe->ShutDown();
			// cout<<" The pipe for SelectPipe is now closed"<<endl;
			// goto pipelabel;

		}
		else if(eng.Compare(r, sp->literal, sp->selOp)==1) {
			sp->outPipe->Insert(r);

			cout<<" Records put in pipe :"<<++ct<<endl;
		}
		else
		{
			continue;
		}
	}while(input->Remove(r));


	sp->outPipe->ShutDown();
	cout<<" The pipe for SelectPipe is now closed"<<endl;

	pipelabel:


	cout<<"\n Total records that have been read from the SelectPipe input pipe is "<<totalrecs<<endl;
	delete r;


}

void SelectPipe::Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal){

	cout<<"\n Select Pipe run is called"<<endl;
	//cout<<"called run 222"<<endl;

	//initilize all the variables


	params* sp = new params;

	sp->outPipe = &outPipe;
	sp->selOp = &selOp;

	sp->inPipe = &inPipe;

	cout<<" \nThe CNF for this call is : "<<endl;
	sp->selOp->Print();


	sp->literal = &literal;
	pthread_create(&thread, NULL, pipeexec,(void*) sp);
}

void SelectPipe::WaitUntilDone () {
	int flag=-1;

	flag=pthread_join (thread, NULL);
	if(flag==0)
	cout<<" \n THe select pipe thread is working fine..."<<endl;
	else
	{
	cout<<"\n There was a deadlock detected in thread: "<<"Select Pipe with error number = "<<flag<<endl;
	exit(0);
	}
}

void SelectPipe::Use_n_Pages (int runlen) {
	this->nPages = runlen;
}
























// This is the code for the selectFile class
















void SelectFile::dofile()
{


	//params *sp = (params *) arg;
	int count = 0;
	int count2 = 0;
	cout<<"THe number of calls to select file is :"<<a;



	//cout<<"call to sf"<<endl;

	ComparisonEngine eng;

	//cout<<sp->inFile<<endl;//addr

	Record *r = new Record();
	//sp->literal->Print(tmpRecord);




	DBFile *ptr;

	ptr = this->inFile;


	MovePointer(ptr);

	ptr->MoveFirst();






	if((ptr->GetNext(*r))!=1)
	cout<<" \nTHere was no record detected in the DFile class, check it"<<endl;

	do{


		//cout<<"\nread :"<<++count2<<endl;
		if(eng.Compare(r, this->literal, this->selOp)) {
			//cout <<"\n\n executing selectFile"<<endl;

			//cout<<"\nfound :"<<++count<<endl;
			++count;
			if(count==99)
			{
			// 	cout<< "\nThe output pipe in selectfile got full, remedy this"<<endl;
			// goto label;
		}
			this->outPipe->Insert(r);
			Record *tmp = new Record();

		}
	}while(ptr->GetNext(*r));


	label:
	delete r;
	//delete tmp;

	this->outPipe->ShutDown();
	cout<<"Selectfile outPipe is now closed foe write, written "<<count<<endl;
	a--;

}







void SelectFile::MovePointer(DBFile* p)
{		//make sure the pointer is in the beginig of the db file
		//thois function can be edited to add more pointer calls to move them into start postions

		p->MoveFirst();


}






void *SelectFile::exec(void *arg) {


	SelectFile *f = (SelectFile *) arg;
	f->dofile();


	// params *sp = (params *) arg;
	// int count = 0;
	// int count2 = 0;
	// cout<<"THe number of calls to select file is :"<<a;
	//
	//
	//
	// //cout<<"call to sf"<<endl;
	//
	// ComparisonEngine eng;
	//
	// //cout<<sp->inFile<<endl;//addr
	//
	// Record *r = new Record();
	// //sp->literal->Print(tmpRecord);
	//
	// cout <<"\n\n executing selectFile"<<endl;
	//
	//
	// DBFile *ptr;
	//
	// ptr = sp->inFile;
	//
	// MovePointer(ptr);
	//
	// ptr->MoveFirst();
	//
	// if((ptr->GetNext(*r))!=1)
	// cout<<" \nTHere was no record detected in the DFile class, check it"<<endl;
	//
	// do{
	//
	// 	//cout<<"\nread :"<<++count2<<endl;
	// 	if(eng.Compare(r, sp->literal, sp->selOp)) {
	//
	// 		//cout<<"\nfound :"<<++count<<endl;
	// 		++count;
	// 		if(count==99)
	// 		{
	// 		// 	cout<< "\nThe output pipe in selectfile got full, remedy this"<<endl;
	// 		// goto label;
	// 	}
	// 		sp->outPipe->Insert(r);
	// 		Record *tmp = new Record();
	//
	// 	}
	// }while(ptr->GetNext(*r));
	//
	//
	// label:
	// delete r;
	// //delete tmp;
	//
	// sp->outPipe->ShutDown();
	// cout<<"Selectfile outPipe is now closed foe write, written "<<count<<endl;
	// a--;
}

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {

	cout<<"\n \n The select file run was called"<<endl;

	a++;


	params* sp = new params;



	sp->inFile = &inFile;
	this->inFile= &inFile;
	sp->outPipe = &outPipe;
	this->outPipe = &outPipe;
	sp->selOp = &selOp;
	this->selOp = &selOp;
	cout<<" \nThe CNF for this call is : "<<endl;
	this->selOp->Print();
	sp->literal = &literal;
	this->literal = &literal;



	int flag=-1;

	flag=pthread_create(&thread, NULL, exec,this);
	if(flag==0)
	cout<<" \n THe selectFile thread was created"<<endl;
	else
	{
	cout<<"\n There was a creation problem in thread: "<<"Select FIle with error number = "<<flag<<endl;
	exit(0);
	}





}

void SelectFile::WaitUntilDone () {

	int flag=-1;
	while(a>1)
	{};

	flag=pthread_join (thread, NULL);
	if(flag==0)
	cout<<" \n THe select file thread is working fine..."<<endl;
	else
	{
	cout<<"\n There was a deadlock detected in thread: "<<"Select FIle with error number = "<<flag<<endl;
	exit(0);
	}
}

void SelectFile::Use_n_Pages (int runlen) {
	this->nPages = runlen;
}








// Project class function definitions



void Project::shut(Pipe *p)
{
	p->ShutDown();
}







void *Project::projectexec(void *arg) {


	cout<<" \nTHe projecr thread was created sucessfully"<<endl;
	params *sp = (params *) arg;

	Record *r = new Record;

	int a=0;

	Pipe *input;
	input=sp->inPipe;

	if(input->Remove(r)!=1)
	{
		cout<< "THere was a problem in the projject input pipe, no data found"<<endl;
		exit(0);
	}

	do{
		r->Project(sp->keepMe, sp->numAttsOutput, sp->numAttsInput);
		sp->outPipe->Insert(r);
		a++;
	}while(input->Remove(r)==1);

	cout<<"\nTotal number of records that were inserted into the output pipe of project is: "<<a<<endl;
	shut(sp->outPipe);
	//sp->outPipe->ShutDown();
	cout<<"\n The output Pipe of project is now closed"<<endl;

	// while(sp->outPipe->Remove(tmpRcd)) {
	// 	//tmpRcd->Project(sp->keepMe, sp->numAttsOutput, sp->numAttsInput);
	// 	sp->inPipe->Insert(tmpRcd);
	// 	a++;
	// }
	//
	// sp->inPipe->ShutDown();

	delete r;


	cout<<"\n\nThe toal number of records that are projected "<<a<<endl;
	//return;
}

void Project::Run (Pipe &inPipe, Pipe &outPipe, int *keepMe,
		int numAttsInput, int numAttsOutput) {

			cout<<"\nRun called for Project "<<endl;

	params* sp = new params;

	sp->inPipe = &inPipe;
	sp->outPipe = &outPipe;
	sp->keepMe = keepMe;
	cout<<sp->keepMe[0]<<endl;
	cout<<sp->keepMe[0]<<endl;
	sp->numAttsInput = numAttsInput;
	cout<<sp->numAttsInput<<endl;
	sp->numAttsOutput = numAttsOutput;
	cout<<sp->numAttsOutput<<endl;


	pthread_create(&thread, NULL, projectexec, sp);


}

void Project::WaitUntilDone () {
	int flag=-1;

	flag=pthread_join(thread, NULL);
	if(flag==0)
	cout<<" \n THe project run thread is working fine..."<<endl;
	else
	{
	cout<<"\n There was a deadlock detected in thread: "<<"Project with error number = "<<flag<<endl;
	exit(0);
	}
}

void Project::Use_n_Pages (int n) {
	this->nPages = n;
}










// The function definitons for Duplicate Removal class







void *DuplicateRemoval::duprem(void *arg){
	DuplicateRemoval *dr = (DuplicateRemoval *) arg;
	dr->DoDuplicateRemoval();
	return NULL;





// 	params *sp = (params *) arg;
// 	OrderMaker *om = new OrderMaker(sp->mySchema);
// 	cout <<"duplicate removal on ordermaker: "; om->Print();
// //	Attribute *atts = this->mySchema->GetAtts();
// 		// loop through all of the attributes, and list as it is
//
// //	cout <<"distince on ";
// //	om->Print(); cout <<endl;
// 	if(!om)
// 		cerr <<"Can not allocate ordermaker!"<<endl;
// 	Pipe sortPipe(1000);
// //	int runlen = 50;//temp...
// 	int size=nPages;
// 	BigQ *bigQ = new BigQ(*(sp->inPipe), sortPipe, *om, size);
//
// 	ComparisonEngine cmp;
// 	Record *tmp = new Record();
// 	Record *chk = new Record();
// 	if(sortPipe.Remove(tmp)) {
// 		//insert the first one
// 		bool more = true;
// 		while(more) {
// 			more = false;
// 			Record *copyMe = new Record();
// 			copyMe->Copy(tmp);
// 			sp->outPipe->Insert(copyMe);
// 			while(sortPipe.Remove(chk)) {
// 				if(cmp.Compare(tmp, chk, om) != 0) { //equal
// 					tmp->Copy(chk);
// 					more = true;
// 					break;
// 				}
// 			}
// 		}
// 	}
//
// //	sortPipe.ShutDown();
// 	sp->outPipe->ShutDown();
//
//
//
//
//
// 	return NULL;
}


void DuplicateRemoval::ClearSortedPipe(Pipe *P, OrderMaker *om){

	ComparisonEngine eng;
	Record *tmp = new Record();
	Record *next = new Record();
	Record *cpy=new Record();
	int a=0;
	f:
	if(P->Remove(tmp)==1) {
		cpy->Copy(tmp);
		this->outPipe->Insert(tmp);
		//insert the first one
		//bool more = true;
		//while(a==0) {
			//more = false;
			// Record *copyMe = new Record();
			//copyMe->Copy(tmp);
			//this->outPipe->Insert(copyMe);
			while(P->Remove(next)==1) {

				if(eng.Compare(cpy, next, om) == 0) { //equal
					continue;
				}
				else
				{
					//this->outPipe->Insert(next);
					goto f;
				}

					//more = true;
					//break;
				}
			}





	P->ShutDown();

	//sortPipe.ShutDown();
	this->outPipe->ShutDown();








}

void DuplicateRemoval::DoDuplicateRemoval(){




	//OrderMaker *om = new OrderMaker(this->mySchema);

	OrderMaker om(this->mySchema);
	cout <<"duplicate removal on ordermaker: ";
	om.Print();
//	Attribute *atts = this->mySchema->GetAtts();
		// loop through all of the attributes, and list as it is

//	cout <<"distince on ";
//	om->Print(); cout <<endl;
	 // if(!om)
	 // 	cerr <<"Can not allocate ordermaker!"<<endl;
	Pipe sortPipe(100000);
  // Pipe *sortPipe;
	// sortPipe= new Pipe(1000);
	// Record *r=new Record();
	//
	// cout<< " The pulled record display here"<<endl;
	//
	// this->inPipe->Remove(r);
	// r->Print(this->mySchema);


	//cout<<" checking duplicate"<<endl;
//	int runlen = 50;//temp...


	 BigQ B(*(this->inPipe), sortPipe, om, 100);
	 //sortPipe.ShutDown();

	cout<<"BIGQ working"<<endl;
	//BigQ L(*inPipeL, *LO, *omL, this->nPages);



	ClearSortedPipe(&sortPipe, &om);



	// ComparisonEngine cmp;
	// Record *tmp = new Record();
	// Record *chk = new Record();
	// if(this->inPipe->Remove(tmp)) {
	// 	//insert the first one
	// 	bool more = true;
	// 	while(more) {
	// 		more = false;
	// 		Record *copyMe = new Record();
	// 		copyMe->Copy(tmp);
	// 		this->outPipe->Insert(copyMe);
	// 		while(this->inPipe->Remove(chk)) {
	// 			if(cmp.Compare(tmp, chk, &om) != 0) { //equal
	// 				tmp->Copy(chk);
	// 				more = true;
	// 				break;
	// 			}
	// 		}
	// 	}
	// }
	//
	 sortPipe.ShutDown();
	// this->outPipe->ShutDown();







}

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {

	params* sp = new params;
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->mySchema = &mySchema;
	pthread_create(&thread,NULL,duprem,this);
}

void DuplicateRemoval::WaitUntilDone () {
	int flag=-1;

	flag=pthread_join(thread, NULL);
	if(flag==0)
	cout<<" \n THe duplicaterem thread is working fine..."<<endl;
	else
	{
	cout<<"\n There was a deadlock detected in thread: "<<"Duplicate removal with error number = "<<flag<<endl;
	exit(0);
	}
}

void DuplicateRemoval::Use_n_Pages (int n) {
   this->nPages = n;
}


/*class Join : public RelationalOp {
	public:
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) { }
	void WaitUntilDone () { }
	void Use_n_Pages (int n) { }
};*/

void Join::Run(Pipe& inPipeL, Pipe& inPipeR, Pipe& outPipe, CNF& selOp, Record& literal) {
  PACK_ARGS6(param, &inPipeL, &inPipeR, &outPipe, &selOp, &literal, runLength);
  FATALIF(create_joinable_thread(&worker, work, param), "Error creating worker thread.");
}

void* Join::work(void* param) {
  UNPACK_ARGS6(Args, param, pleft, pright, pout, sel, lit, runLen);
  OrderMaker orderLeft, orderRight;
  if (sel->GetSortOrders(orderLeft, orderRight))
    sortMergeJoin(pleft, &orderLeft, pright, &orderRight, pout, sel, lit, runLen);
  else nestedLoopJoin(pleft, pright, pout, sel, lit, runLen);
  pout->ShutDown();
}

void Join::sortMergeJoin(Pipe* pleft, OrderMaker* orderLeft, Pipe* pright, OrderMaker* orderRight, Pipe* pout,
                          CNF* sel, Record* literal, size_t runLen) {
  ComparisonEngine cmp;
  Pipe sortedLeft(PIPE_SIZE), sortedRight(PIPE_SIZE);
  BigQ qLeft(*pleft, sortedLeft, *orderLeft, runLen), qRight(*pright, sortedRight, *orderRight, runLen);
  Record fromLeft, fromRight, merged, previous;
  JoinBuffer buffer(runLen);

  // two-way merge join
  for (bool moreLeft = sortedLeft.Remove(&fromLeft), moreRight = sortedRight.Remove(&fromRight); moreLeft && moreRight; ) {
    int result = cmp.Compare(&fromLeft, orderLeft, &fromRight, orderRight);
    if (result<0) moreLeft = sortedLeft.Remove(&fromLeft);
    else if (result>0) moreRight = sortedRight.Remove(&fromRight);
    else {       // equal attributes: fromLeft == fromRight ==> do joining
      buffer.clear();
      for(previous.Consume(&fromLeft);
          (moreLeft=sortedLeft.Remove(&fromLeft)) && cmp.Compare(&previous, &fromLeft, orderLeft)==0; previous.Consume(&fromLeft))
					{
        FATALIF(!buffer.add(previous), "Join buffer exhausted.");
			   // gather records of the same value

				 indexing(pleft);
			 }
      FATALIF(!buffer.add(previous), "Join buffer exhausted.");     // remember the last one
      do {       // Join records from right pipe
        FOREACH(rec, buffer.buffer, buffer.nrecords)
          if (cmp.Compare(&rec, &fromRight, literal, sel)) {   // actural join
            merged.CrossProduct(&rec, &fromRight);
            pout->Insert(&merged);
						indexing(pright);
          }
        END_FOREACH
      } while ((moreRight=sortedRight.Remove(&fromRight)) && cmp.Compare(buffer.buffer, orderLeft, &fromRight, orderRight)==0);    // read all records from right pipe with equal value
    }
  }
}

void Join::nestedLoopJoin(Pipe* pleft, Pipe* pright, Pipe* pout, CNF* sel, Record* literal, size_t runLen) {
//   DBFile rightFile;
//   dumpFile(*pright, rightFile);
//   JoinBuffer leftBuffer(runLen);
//
//   // nested loops join
//   FOREACH_INPIPE(rec, pleft)
//     if (!leftBuffer.add(rec)) {  // buffer full ==> do join
//       joinBuf(leftBuffer, rightFile, *pout, *literal, *sel);
//       leftBuffer.clear();       // start next chunk of LEFT
//       leftBuffer.add(rec);
//     }
//   joinBuf(leftBuffer, rightFile, *pout, *literal, *sel);   // join the last buffer
//   rightFile.Close();
// }
//
// void Join::joinBuf(JoinBuffer& buffer, DBFile& file, Pipe& out, Record& literal, CNF& selOp) {
//   ComparisonEngine cmp;
//   Record merged;
//
//   FOREACH_INFILE(fromFile, file) {
//     FOREACH(fromBuffer, buffer.buffer, buffer.nrecords)
//       if (cmp.Compare(&fromBuffer, &fromFile, &literal, &selOp)) {   // actural join
//         merged.CrossProduct(&fromBuffer, &fromFile);
//         out.Insert(&merged);
//       }
//     END_FOREACH
//   }
}

void Join::dumpFile(Pipe& in, DBFile& out) {
  const int RLEN = 10;
  char rstr[RLEN];
  Rstring::gen(rstr, RLEN);  // need a random name otherwise two or more joins would crash
  std::string tmpName("join");
  tmpName = tmpName + rstr + ".tmp";
  out.Create((char*)tmpName.c_str(), HEAP, NULL);
  Record rec;
  while (in.Remove(&rec)) out.Add(rec);
}

int Join::indexing(void *s)
	{

		info_struct *args = (info_struct *)s;
		int num_runs =100;
		int counter=0;

		int fin[num_runs][num_runs];
		int c_i[num_runs][num_runs];
		int index[num_runs][2];

		int arr[num_runs][num_runs];

		for(int i=0;i<num_runs;i++)
		{
			for(int j=0;j<num_runs;j++)
			{
				arr[i][j]=0;
			}
		}



		while(counter<num_runs)
		{
			for(int n=0;n<num_runs;n++)
			{
			// fin[counter][n]=arr[counter][counter];
			// c_i[counter][n]=arr[counter][counter];
			}
			counter++;
		}




		for(int i=0;i<num_runs-1;i++){

		counter= 1+((*(args->run_length))*i);
		counter= (*(args->run_length))*(i+1);
		}

		// index[num_runs-1][start] = 1+((*(args->run_length))*(num_runs-1));
		// index[num_runs-1][end] = gp_index-1;





		int flag_for_file_size=0;

		{
			if(arr[num_runs-1][num_runs-1]==0)
			{


				// 	if(flag_for_file_size==0)
				// 	{
				// 		flag_for_file_size=arr[0][0];
				// 		cout<< "file size not exceeded";
				// 		cout<<endl;
				// 	}
				// 	else
				// 	{
				// 		cout<<"error at 610 line, code aborted there";
				// 		goto label;

			}



		}








	}

















//Sum class definitions here .........





Record* Sum::Makerec(Type type, int intSum, double doubleSum)
{

	Record *outrec=new Record();

	if (type == Int) {

					// result << intSum;
					// resultSum = result.str();
					// resultSum.append("|");
					stringstream ss;
					ss << intSum;
					string s=ss.str();
					s.append("|");


					Attribute IA = {"int", Int};
					Schema out_sch("out_sch", 1, &IA);
					outrec->ComposeRecord(&out_sch, s.c_str());
					// resultRec.ComposeRecord(&out_sch, ss.str().c_str());

	} else {

					// result << doubleSum;
					// resultSum = result.str();
					// resultSum.append("|");


					stringstream ss;
					ss << doubleSum;
					string s=ss.str();
					s.append("|");

					Attribute DA = {"double", Double};
					Schema out_sch("out_sch", 1, &DA);
					outrec->ComposeRecord(&out_sch, s.c_str());
					// resultRec.ComposeRecord(&out_sch, ss.str().c_str());
	}




	return outrec;




}











void Sum::doSum() {

        cout << "Starting Summation process in the do sum function" << endl;

        //struct RunParams* params = (struct RunParams*) parameters;

				Record *rec=new Record();
				Record *outrec;
        Function *function = this->function;

        int intSum = 0;
        double doubleSum = 0.0;
        int intAttrVal = 0;
        double doubleAttrVal = 0.0;

        Type type;
				int i=0;

        while (inPipe->Remove(rec)==1) {
					cout<<"\n the rem :          \n "<<i++<<endl;


                type = function->Apply(*rec, intAttrVal, doubleAttrVal);

                if (type == Int) {
                        intSum += intAttrVal;

                } else {
                        doubleSum += doubleAttrVal;
                }
        }



        // ostringstream result;
        // string resultSum;
        // Record resultRec;

        // create output record




				outrec=this->Makerec(type, intSum, doubleSum);


	        // if (type == Int) {
					//
	        //         // result << intSum;
	        //         // resultSum = result.str();
	        //         // resultSum.append("|");
					// 				stringstream ss;
					// 				ss << intSum;
					// 				string s=ss.str();
					// 				s.append("|");
					//
					//
	        //         Attribute IA = {"int", Int};
	        //         Schema out_sch("out_sch", 1, &IA);
	        //         outrec->ComposeRecord(&out_sch, s.c_str());
					// 				// resultRec.ComposeRecord(&out_sch, ss.str().c_str());
					//
	        // } else {
					//
	        //         // result << doubleSum;
	        //         // resultSum = result.str();
	        //         // resultSum.append("|");
					//
					//
					// 				stringstream ss;
					// 				ss << doubleSum;
					// 				string s=ss.str();
					// 				s.append("|");
					//
	        //         Attribute DA = {"double", Double};
	        //         Schema out_sch("out_sch", 1, &DA);
					// 				outrec->ComposeRecord(&out_sch, s.c_str());
	        //         // resultRec.ComposeRecord(&out_sch, ss.str().c_str());
	        // }

        outPipe->Insert(outrec);
        outPipe->ShutDown();


				cout<<"Done summation"<<endl;

}



void* Sum::dot(void* arg){

	Sum *s = (Sum *) arg;
	s->doSum();

}

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe) {

        cout << "In Summation Run" << endl;
        //struct RunParams* params = (RunParams*) malloc(sizeof (RunParams));
        this->inPipe = &inPipe;
        this->outPipe = &outPipe;
        this->function = &computeMe;

        pthread_create(&thread, NULL, dot, this);
}

void Sum::WaitUntilDone() {

        pthread_join(thread, NULL);
        //cout << "\n Summation Complete" << endl;

}

void Sum::Use_n_Pages(int n) {

        cout << "Setting run length in use n pages : " << n << endl;
        this->nPages = n;
}



void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
  PACK_ARGS5(param, &inPipe, &outPipe, &groupAtts, &computeMe, runLength);
  FATALIF(create_joinable_thread(&worker, work, param), "Error creating worker thread.");
}

void* GroupBy::work(void* param) {
  UNPACK_ARGS5(Args, param, in, out, order, func, runLen);
  if (func->resultType() == Int) doGroup<int>(in, out, order, func, runLen);
  else doGroup<double>(in, out, order, func, runLen);
  out->ShutDown();
}






//definitons for the class write out

	void WriteOut::Run (Pipe &inPipe, FILE *outFile, Schema &mySchema) {



		Record *r= new Record();


		while(inPipe.Remove(r)==1)
		{
			r->WriteToFile(outFile,&mySchema);
		}

		fclose(outFile);









	}
	void WriteOut::WaitUntilDone (){

		pthread_join(thread, NULL);
		//cout << "\n Group By Complete" << endl;
		//return NULL;
	}
	void WriteOut::Use_n_Pages (int n){


		this->nPages = n;
	}




	int RelationalOp::create_joinable_thread(pthread_t *thread,
	                                         void *(*start_routine) (void *), void *arg) {
	  pthread_attr_t attr;
	  pthread_attr_init(&attr);
	  pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
	  int rc = pthread_create(thread, &attr, start_routine, arg);
	  pthread_attr_destroy(&attr);
	  return rc;
	}

	JoinBuffer::JoinBuffer(size_t npages): size(0), capacity(PAGE_SIZE*npages), nrecords(0) {
	  buffer = new Record[PAGE_SIZE*npages/sizeof(Record*)];
	}

	JoinBuffer::~JoinBuffer() { delete[] buffer; }

	bool JoinBuffer::add (Record& addme) {
	  if((size+=addme.getLength())>capacity) return 0;
	  buffer[nrecords++].Consume(&addme);
	  return 1;
	}
	void RelationalOp::WaitUntilDone() {
	  FATALIF(pthread_join (worker, NULL), "joining threads failed.");
	}
