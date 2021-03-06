#include <stdio.h>
#include <stdlib.h>
#include "../include/psum.h"
#include "../include/histogram.h"

void Psum(histogram * hist, psum * ps,int64_t start)
{
	// histogram and psum have the same size
	uint64_t i = 1;
	ps -> num_tuples = hist -> num_tuples;

	if((ps -> psum_tuples = (psum_tuple *)malloc(ps -> num_tuples * sizeof(psum_tuple))) == NULL)
	{
		perror("psum.c , first malloc\n");
		exit(-1);
	}

	ps -> psum_tuples[0].key = 0; //first
	ps -> psum_tuples[0].payload = 0;
	ps -> psum_tuples[0].position = start;

	while(i < ps->num_tuples)
	{
		ps -> psum_tuples[i].position = ps -> psum_tuples[i-1].position + hist -> hist_tuples[i-1].sum;
		ps -> psum_tuples[i].payload = i;
		ps -> psum_tuples[i].key = hist -> hist_tuples[i].key;
		i++;
	}
}

void Print_Psum(histogram * hist, psum * ps)
{
    uint64_t i = 0;
	printf("-------------------> Psum <-------------------\n");
    while(i < ps -> num_tuples)
    {
		if(hist -> hist_tuples[i].sum != 0)
        printf("RowID = %ld\tBucket = %ld\tPosition = %ld\n",ps -> psum_tuples[i].payload ,ps -> psum_tuples[i].key,    ps -> psum_tuples[i].position);
        i++;
    }
	printf("----------------------------------------------\n");
}

// Set positions back to original
void RestorePsum(histogram *hist, psum *ps)
{
 	uint64_t i = 0;
	while(i < ps->num_tuples)
	{
		ps->psum_tuples[i].position = ps->psum_tuples[i].position - hist->hist_tuples[i].sum;
		i++;
	}
}
