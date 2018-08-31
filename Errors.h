#ifndef _ERRORS_H_
#define _ERRORS_H_

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

/* macro to halt the program if a condition is satisfied */
#define FATALIF(expr,msg...) {\
	if (expr) {\
		printf("FATAL [%s:%d] ", __FILE__, __LINE__);\
		printf(msg);\
		printf("\n");\
		assert(1==2);\
		exit(-1);\
	}\
}


#endif //_ERRORS_H_
