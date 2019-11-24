#ifndef _INTERVENING_H
#define _INTERVENING_H

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include "relation.h"


typedef struct intervening_array{
	uint64_t position;
	uint64_t *payload_array;
}intervening_array;

typedef struct intervening_struct{
	uint64_t total_arrays;
	intervening_array **rowId;
}intervening_struct;

intervening_array *interveningArrayInit();
intervening_struct *interveningStructInit();

intervening_struct *updateInterveningStruct(intervening_struct *intervening_struct, uint64_t *array_A, uint64_t *array_B);
void pushJoinedElements_v2(uint64_t payload_A, uint64_t payload_B, intervening_array *array_A, intervening_array *array_B);
void Join_v2(intervening_struct *intervening_struct, relation *rel_A, relation *rel_B, intervening_array *array_A, intervening_array *array_B);


#endif