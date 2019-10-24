#ifndef _QUICKSORT_H
#define _QUICKSORT_H

#include <stdint.h>
#include "../include/psum.h"

void swap(tuple *a, tuple *b);
int partition(relation *R, int start, int end);
void Quicksort(relation *R, int start, int end);
void printBucket(relation *R, int start, int end);

#endif