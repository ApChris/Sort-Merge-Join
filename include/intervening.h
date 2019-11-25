#ifndef _INTERVENING_H
#define _INTERVENING_H

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include "relation.h"


typedef struct intervening_array
{
	uint64_t position;
	uint64_t * payload_array;
}intervening_array;

typedef struct intervening
{
	uint64_t total_arrays;
	intervening_array * rowId;
}intervening;

intervening_array * intervening_Array_Init();
intervening * intervening_Init();

intervening * updateInterveningStruct(intervening *intervening, uint64_t *array_A, uint64_t *array_B);
void pushJoinedElements_v2(uint64_t payload, intervening_array * array);
uint64_t Join_v2(intervening *intervening, relation *rel_A, relation *rel_B, intervening_array *array_A, intervening_array *array_B);

uint64_t binarySearch(uint64_t * array,uint64_t start, uint64_t end, uint64_t payload);
uint64_t Sum_Column(intervening_array * array);
#endif
