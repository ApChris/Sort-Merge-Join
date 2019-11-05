#ifndef _QUICKSORT_H
#define _QUICKSORT_H

#include <stdint.h>
#include "psum.h"

void swap(tuple *a, tuple *b);
int64_t partition(relation * rel, int64_t start, int64_t end);
void Quicksort(relation * rel, int64_t start, int64_t end);

#endif
