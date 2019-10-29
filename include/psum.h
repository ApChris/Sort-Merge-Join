#ifndef _PSUM_H
#define _PSUM_H

#include <stdint.h>
#include "../include/histogram.h"

typedef struct psum_tuple
{
    uint64_t key;
    uint64_t payload;
    uint64_t position;
}psum_tuple;

typedef struct psum
{
    psum_tuple * psum_tuples;
    uint64_t num_tuples;
}psum;

void Psum(histogram * hist, psum * ps, int64_t start);
void Print_Psum(histogram * hist, psum * ps);
void RestorePsum(histogram *hist, psum *ps);

#endif
