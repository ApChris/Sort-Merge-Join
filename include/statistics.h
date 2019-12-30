#ifndef _STATISTICS_H_
#define _STATISTICS_H_
#include <stdio.h>
#include <stdbool.h>
#include <inttypes.h>
#include "metadata.h"

#define N 50000000

typedef struct statistics
{
	uint64_t * la;	// min value of row A
	uint64_t * ua;	// max value of row A
	double * fa;	// # values in row A
	double * da; 	// # distinct values in row A
	uint64_t * size_da;
}statistics;

statistics * Calculate_Statistics(metadata * md, uint64_t rows);

#endif
