#include <stdio.h>
#include <stdlib.h>
#include "../include/relation.h"

void swap(tuple *a, tuple *b){
    tuple temp = *a;
    *a = *b;
    *b = temp;
}

int partition(relation *R, int start, int end){
	uint64_t pivot = R->tuples[end].key;
	int i = start-1;

	for (int j = start; j <= end - 1; j++)
	{
		if (R->tuples[j].key < pivot)
		{
			i++;
			swap(&R->tuples[i], &R->tuples[j]);
		}
	}
	swap(&R->tuples[i+1], &R->tuples[end]);
	return (i+1);
}

void Quicksort(relation *R, int start, int end){

	if (start < end)
	{
		int pi = partition(R, start, end);
		Quicksort(R, start, pi-1);
		Quicksort(R, pi+1, end);
	}
}

void printBucket(relation *R, int start, int end){
	for (int i = start; i < end; i++)
	{
		printf("%ld, %ld\n", R->tuples[i].key, R->tuples[i].payload);
	}
}