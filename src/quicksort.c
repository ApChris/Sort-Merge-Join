#include <stdio.h>
#include <stdlib.h>
#include "../include/relation.h"

void swap(tuple * a, tuple * b)
{
    tuple  temp  = *a;
    *a = *b;
    *b = temp;

}

int64_t partition(relation * rel, int64_t start, int64_t end)
{
	uint64_t pivot = rel -> tuples[end].key;

	int64_t i = start - 1;

	for (int64_t j = start ; j <= end - 1; j++)
	{
		if (rel -> tuples[j].key < pivot)
		{
			i++;
			swap(&rel -> tuples[i], &rel -> tuples[j]);
		}
	}
	swap(&rel -> tuples[i+1], &rel -> tuples[end]);

	return (i+1);
}


void Quicksort(relation * rel, int64_t start, int64_t end)
{

         if (start < end)
         {
             int64_t partition_index = partition(rel, start, end);
             Quicksort(rel, start, partition_index - 1);
             Quicksort(rel, partition_index + 1, end);
         }

}

void printBucket(relation * rel, int64_t start, int64_t end)
{
	for(int64_t i = start; i < end; i++)
	{
		printf("%ld, %ld\n", rel -> tuples[i].key, rel -> tuples[i].payload);
	}
}
