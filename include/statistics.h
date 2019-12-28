#ifndef _STATISTICS_H
#define _STATISTICS_H
#include <stdio.h>
#include <stdbool.h>
#include <inttypes.h>
#include "metadata.h"
#include "executeQuery.h"
#define N 50000000

typedef struct statistics
{
	uint64_t * Ia;	// min value of row A
	uint64_t * ua;	// max value of row A
	uint64_t * fa;	// # values in row A
	bool ** da; 	// # distinct values in row A
	uint64_t * size_da;
	uint64_t * num_da;
}statistics;

statistics * Calculate_Statistics(metadata * md, uint64_t rows);

#endif
