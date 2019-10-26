#ifndef _QUICKSORT_H
#define _QUICKSORT_H

#include <stdint.h>
#include "../include/psum.h"

void swap(tuple **a, tuple **b);
uint64_t choose_pivot(uint64_t start, uint64_t end);
uint64_t partition(relation * rel, int64_t start, int64_t end);
void Quicksort(relation * rel, int64_t start, int64_t end);
void printBucket(relation * rel, int64_t start, int64_t end);

#endif
