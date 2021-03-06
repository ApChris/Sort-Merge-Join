#ifndef _QUICKSORT_H
#define _QUICKSORT_H

#include <stdint.h>
#include "psum.h"

void swap(tuple *a, tuple *b);
int64_t partition(relation * rel, int64_t start, int64_t end);
void Quicksort(relation * rel, int64_t start, int64_t end);

void swap_v2(uint64_t * a, uint64_t * b);
int64_t partition_v2(uint64_t * array, int64_t start, int64_t end);
void Quicksort_v2(uint64_t * array, int64_t start, int64_t end);
#endif
