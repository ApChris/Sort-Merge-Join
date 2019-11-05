#ifndef _HISTOGRAM_H
#define _HISTOGRAM_H

#include <stdint.h>
#include "relation.h"
typedef struct hist_tuple
{
    uint64_t key;
    uint64_t payload;
    uint64_t sum;
}hist_tuple;


typedef struct histogram
{
    hist_tuple * hist_tuples;
    uint64_t num_tuples;
}histogram;

void Histogram(relation * rel, histogram * hist, uint64_t sel_byte,uint64_t start,uint64_t end);
void Print_Histogram(histogram * hist);
#endif
