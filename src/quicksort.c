#include <stdio.h>
#include <stdlib.h>
#include "../include/relation.h"

void swap(tuple ** a, tuple ** b)
{

    tuple * temp  = *a;

    *a = *b;
    *b = temp;

}

uint64_t partition(relation * rel, int64_t start, int64_t end)
{
	int64_t pivot = rel -> tuples[end].key;
	int64_t i = start - 1;


	for (uint64_t j = start; j <= end - 1; j++)
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

uint64_t choose_pivot(int64_t start, int64_t end)
{
    return ((start+end) / 2);
}

void Quicksort(relation * rel, int start, int end)
{
	if (start < end)
	{
		int64_t partition_index = partition(rel, start, end);
		Quicksort(rel, start, partition_index - 1);
		Quicksort(rel, partition_index + 1, end);
	}
    // uint64_t key, i, j, pivot;
    // if(start < end)
    // {
    //     pivot = choose_pivot(start,end);
    //     swap(&rel->tuples[start].key,&rel->tuples[end].key);
    //     //swap(&rel->tuples[start].payload,&rel->tuples[end].payload);
    //     key = rel->tuples[start].key;
    //     i = start + 1;
    //     j = end;
    //     while(i <= j)
    //     {
    //         while((i <= end) && (rel -> tuples[i].key <= key))
    //         {
    //             i++;
    //         }
    //         while((j >= start) && (rel -> tuples[j].key > key))
    //         {
    //             j--;
    //         }
    //         if(i < j)
    //         {
    //             swap(&rel->tuples[i].key,&rel->tuples[j].key);
    //             swap(&rel->tuples[i].payload,&rel->tuples[j].payload);
    //         }
    //     }
    //
    //     swap(&rel->tuples[start].key,&rel->tuples[j].key);
    //     swap(&rel->tuples[start].payload,&rel->tuples[j].payload);
    //     Quicksort(rel, start, j - 1);
    //
    //  	Quicksort(rel, j + 1, end);
    //
    // }

}

void printBucket(relation * rel, uint64_t start, uint64_t end)
{
	for(uint64_t i = start; i < end; i++)
	{
		printf("%ld, %ld\n", rel -> tuples[i].key, rel -> tuples[i].payload);
	}
}
