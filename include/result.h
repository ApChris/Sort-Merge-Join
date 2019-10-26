#ifndef _RESULT_H
#define _RESULT_H

#include <stdint.h>
#include "../include/relation.h"


#define ROWS ((1024*1024) / 40)	/*	40:	3 columns of int64_t
								 		1 pointer to the next result, 
										1 int64_t num_tuples	*/

typedef struct result{
	uint64_t buffer[ROWS][3];	//key, rowIdR, rowIdS
	struct result *next_result;
	uint64_t num_results;
} result;


result * resultInit();
void insertNewResult(result *res);
void pushJoinedElements(result *res, uint64_t key, uint64_t rowIdS, uint64_t rowIdR);
void printResult(result *res);
void Join(relation *relation_R, relation *relation_S, result *res);

#endif