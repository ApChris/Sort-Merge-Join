#ifndef _INTERVENING_H
#define _INTERVENING_H

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include "relation.h"


typedef struct intervening
{
	uint64_t position;
	uint64_t * rowId;
	relation * final_rel;
}intervening;

// intervening_array * intervening_Array_Init();
intervening * interveningInit();

uint64_t Join_v2(intervening * final_interv, relation * rel_A, relation * rel_B, uint64_t rowIdA, uint64_t rowIdB);
// intervening * updateInterveningStruct(intervening *intervening, uint64_t *array_A, uint64_t *array_B);
void pushJoinedElements_v2(relation * temp_rel, relation * relation_A, relation * relation_B,uint64_t posA, uint64_t posB, uint64_t counter, uint64_t key);
// uint64_t Join_v2(intervening *intervening, relation *rel_A, relation *rel_B, intervening_array *array_A, intervening_array *array_B);
uint64_t FindRowID(intervening * final_interv, uint64_t rowID);
// uint64_t binarySearch(uint64_t * array,uint64_t start, uint64_t end, uint64_t payload);
// uint64_t Sum_Column(intervening_array * array);
#endif
