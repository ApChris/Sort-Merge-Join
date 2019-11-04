#ifndef _RESULT_H
#define _RESULT_H

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include "../include/relation.h"
#include "../include/psum.h"

#define ROWS ((1024*1024) / 40)	/*	40:	3 columns of int64_t
	  							 		1 pointer to the next result,
	  									1 int64_t num_tuples	*/
#define COLUMNS 3
typedef struct result
{
	uint64_t buffer[ROWS][COLUMNS];	//key, payloadA, payloadB
	struct result *next_result;
	uint64_t num_results;
}result;

result * resultInit();

void insertNewResult(result *res);
uint64_t Join(relation *rel_A, relation *rel_B, result *head);
result * pushJoinedElements(result *head, uint64_t key, uint64_t payload_A, uint64_t payload_B);
void printResult(result *res);
void Deallocate_List(result *res);

#endif
