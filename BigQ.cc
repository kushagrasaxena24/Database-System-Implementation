#include "BigQ.h"
#include "ComparisonEngine.h"
#include <vector>
#include <algorithm>
#include <queue>
#include <time.h>
#include <stdio.h>      
#include <stdlib.h> 
#include <iostream>
#include <ctime>
#include <vector>    
#include <time.h>

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

void* sortThread(void* arg)
{
    bqData *mybqi=new bqData();
    mybqi=(bqData*)arg;
    Pipe *in=mybqi->in;
    Pipe *out=mybqi->out;
	Record myRecord;
	Page myPage;
	Page sortPage;
    OrderMaker *sortorder=mybqi->order;
    int runlen=mybqi->runLength;
    File myFile=mybqi->myFile;
	File currentFile=mybqi->myFile;
    int num=rand()%10244;
	char fileName[50];
	sprintf(fileName,"%d",num);
	myFile.Open(0,fileName);
    int pCount=0,Res=0,rCount=0;
	vector<Record *> myVector;
	vector<int> runL;

    for(;in->Remove(&myRecord);)
	{
	    Record *cRecord=new Record;
	    cRecord->Copy(&myRecord);
		int test=0;

        if(myPage.Append(&myRecord))
	    {
	        myVector.push_back(cRecord);
	    }else{
	        ++pCount;
	        if(pCount==runlen)
	        {
	            stable_sort(myVector.begin(),myVector.end(),CompareR(sortorder));
	            int runLength=0;

vector<Record *>::iterator it=myVector.begin();
                while(it!=myVector.end())
                {
                    if(sortPage.Append(*it))
                    {

                    }else{
                        if(myFile.GetLength()==0)
                        {
                            myFile.AddPage(&sortPage,myFile.GetLength());
                            sortPage.EmptyItOut();
                            sortPage.Append(*it);
                            ++runLength;
                        }else{
                            myFile.AddPage(&sortPage,myFile.GetLength()-1);
                            sortPage.EmptyItOut();
                            sortPage.Append(*it);
                            ++runLength;
                        }
                    }
                    it=it+1;
                }
                if(sortPage.GetNumRecs()==0)
                {

                }else{
                    if(myFile.GetLength()==0)
                    {
                        myFile.AddPage(&sortPage,myFile.GetLength());
                        sortPage.EmptyItOut();
                        ++runLength;
                    }else{
                        myFile.AddPage(&sortPage,myFile.GetLength()-1);
                        sortPage.EmptyItOut();
                        ++runLength;
                    }
                }
                myVector.clear();
                myVector.push_back(cRecord);
                myPage.EmptyItOut();
                myPage.Append(&myRecord);
                pCount=0;
                ++rCount;
                runL.push_back(runLength);
	        }else{
	            myPage.EmptyItOut();
                myPage.Append(&myRecord);
                myVector.push_back(cRecord);
	        }
	    }
	}

	myPage.EmptyItOut();
	sortPage.EmptyItOut();


    
	// OrderMaker* orderr;
	// int x1=sort_old(myPage,500,&orderr, 500, -1);



	if(myVector.empty())
	{
	}else{
		stable_sort(myVector.begin(),myVector.end(),CompareR(sortorder));
		int runLength=0;

vector<Record *>::iterator it=myVector.begin();
		while(it!=myVector.end())
		{
			if(sortPage.Append(*it))
			{

			}else{
				if(myFile.GetLength()==0)
				{
					myFile.AddPage(&sortPage,myFile.GetLength());
					sortPage.EmptyItOut();
					sortPage.Append(*it);
					++runLength;
				}else{
					myFile.AddPage(&sortPage,myFile.GetLength()-1);
					sortPage.EmptyItOut();
					sortPage.Append(*it);
					++runLength;
				}
			}
			++it;
		}
		if(sortPage.GetNumRecs()==0)
		{
			printfunction("Cannot get number of records");
		}else{
			if(myFile.GetLength()==0)
			{
				myFile.AddPage(&sortPage,myFile.GetLength());
				sortPage.EmptyItOut();
				++runLength;
			}else{
				myFile.AddPage(&sortPage,myFile.GetLength()-1);
				sortPage.EmptyItOut();
				++runLength;
			}
		}
		++rCount;
		runL.push_back(runLength);
	}
	

vector<Record *>::iterator it=myVector.begin();
    while(it!=myVector.end())
    {
        delete *it;
        it=it+1;
    }




    myVector.clear();
	myFile.Close();

    myFile.Open(1,fileName);


	priority_queue<RunRecord *, vector<RunRecord *>, CompareQ> myPQ(sortorder);

	Page inputPage[rCount];

	int whichPage[rCount];

	vector<int> runStart;
	for(int i=0;i<rCount;++i)
	{
        int num=0;

vector<int>::iterator it=runL.begin();
        while(it!=runL.begin()+i)
        {
            num+=*it;
            it=it+1;
        }
        runStart.push_back(num);
	}


	int i=0;
	while(i<rCount)
	{
        myFile.GetPage(&(inputPage[i]),runStart[i]);
		whichPage[i]=0;
		i=i+1;
	}
	
	i=0;
	do{
	    RunRecord *myRR=new RunRecord;
		myRR->runNum=i;
		inputPage[i].GetFirst(&(myRR->myRecord));
		myPQ.push(myRR);
		i++;
	}
	while(i<rCount);

	for(;;)
	{
		if(myPQ.empty())
		{
			break;
		}else{
			RunRecord *chRR=myPQ.top();
			myPQ.pop();
			int runNum = chRR->runNum;
			out->Insert(&(chRR->myRecord));
			//get more record from pages
			RunRecord *buRR=new RunRecord;
			if(inputPage[runNum].GetFirst(&(buRR->myRecord)))
			{
			    buRR->runNum=runNum;
				myPQ.push(buRR);
			}else{
				++whichPage[runNum];
				if(whichPage[runNum]>=runL[runNum])
				{

				}else{
				    myFile.GetPage(&(inputPage[runNum]),whichPage[runNum]+runStart[runNum]);
				    inputPage[runNum].GetFirst(&(buRR->myRecord));
				    buRR->runNum=runNum;
				    myPQ.push(buRR);
				}
			}
		}
	}
    //finally shut down the out pipe
    myFile.Close();
    //desFile.Close();
    remove(fileName);
	out->ShutDown ();

}

int BigQ :: indexing(void *s)
	{

	    
		int num_runs =100;
		int counter=0;
		

		int fin[num_runs][num_runs];
		int c_i[num_runs][num_runs];
		int index[num_runs][2];

		int arr[num_runs][num_runs];

 		int i=0;
		while(i<num_runs)


		{  int j=0;
			
			while(j<num_runs)
			{
				arr[i][j]=0;
				j=j+1;
			}
			i=i+1;
		}

	





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

int BigQ :: split(Page *p, int value,OrderMaker *sort_order)
{

	

	int i = p->GetNumRecs();
	int arr1[i/2];
	int arr2[i/2];
	int x1=sort_new(p,500,sort_order, 500, -1);



	std::vector<int> vectorfirst (arr1,arr1+i/2);
	std::vector<int> vectorsecind (arr1+i/2,arr1+i);

	std::partial_sort (vectorfirst.begin(), vectorfirst.begin()+i/2, vectorfirst.end());


	std::partial_sort (vectorsecind.begin()+i/2, vectorsecind.begin()+i, vectorsecind.end(),myfunction);


}
int BigQ::sort_old(Page *p, int value,OrderMaker *sort_order, int a, int b){

int flag=a;
int run=b;
int x1=sort_new(p,500,sort_order, 500, -1);
    if(flag!=0)
{
	struct info_struct_temp {

		Pipe *input;
		Pipe *output;
		
		OrderMaker *sort_order;
		int *run_length;

	}info_temp;
	x1=sort_new(p,500,sort_order, 500, -1);


Two_Pass_Merge_Sort2(&info_temp, flag);


}
 return 0;
}

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	//Initializing the input, output, order marker and run length

	bqData *bq=new bqData();
	bq->in=&in;
	bq->out=&out;
	bq->order=&sortorder;
	bq->currentFile=myFile;
	bq->runLength=runlen;
	bq->myFile=myFile;
	Page *p;
	pthread_t sortthread;
	int res=split(p,1,&sortorder);
	int x1=sort_new(p,500,&sortorder, 500, -1);
	pthread_create(&sortthread,NULL,&sortThread,(void*)bq);


}



int BigQ::sort_new(Page *p, int value,OrderMaker *sort_order, int run, int flag){

	Record *t = new Record();
	if(flag != 100)
	{

		while(value!=0)
		{
/// Add values to record after sorted

char f_path[8]="newfile";
//fp.Open(0,f_path);

int f  = value;
int nex  = 0;
int start = 0;
int end   = 1;
int fin[flag];//set to 0
int c_i[run];
int index[2];


	for(int i=0;i<nex;i++)
	{
		index[i]=nex;
		nex++;
		if(i==1)
		break;

	}
	value=0;



		}
		return 1;
	}
	else
	{
		printfunction("no new record to sort");
		return 0;
	}
	return 0;

}

void* BigQ::Two_Pass_Merge_Sort2(void* arg,int check){
	 if(check!=0){

			// if((p+p_index)->Append(temporary)==1){
			//
			// 	num_recs++;
			// }
			//
			// else if(++p_index == *(args->run_length)){
			//
			// 	cout <<"Run no "<<num_runs<<"\n";
			// 	cout <<"Pages "<<p_index<<"\n";
			// 	cout <<"No of Records "<<num_recs<<"\n\n";



			//
			// 	sort_run(p,num_recs,f_new,gp_index,args->sort_order);
			//
			// 	num_runs++;
			//
			// 	p_index=0;
			// 	num_recs=0;
			//
			// 	(p+p_index)->Append(temporary);
			// 	num_recs++;
			// 	temporary = new Record();



	}
	int test=0;
	for(int i=0;i<5;i++)
	{
	test= indexing(arg);
	}

}


BigQ::~BigQ () {
}
