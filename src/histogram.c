#include <stdio.h>
#include <stdlib.h>
#include "../include/histogram.h"


void Histogram(relation * rel, histogram * hist, uint64_t sel_byte,uint64_t start,uint64_t end)
{

    uint64_t i = 0;
    uint64_t result = 0;

    if((hist -> hist_tuples = (hist_tuple *)malloc(256 * sizeof(hist_tuple))) == NULL)
    {
        perror("histogram.c , first malloc\n");
        exit(-1);
    }
    hist -> num_tuples = 256;

    while(i < hist -> num_tuples)
    {
        hist -> hist_tuples[i].key = i;
        hist -> hist_tuples[i].payload = i;
        hist -> hist_tuples[i].sum = 0;
        i++;
    }

    i = start;
    while(i < end)
    {
        result = (rel -> tuples[i].key >> (8*sel_byte) & 0xFF);

        hist -> hist_tuples[result].sum++;
        i++;
    }

}


void Print_Histogram(histogram * hist)
{
    uint64_t i = 0;
    printf("----------------> Histogram <----------------\n");
    while(i < hist -> num_tuples)
    {
        if(hist -> hist_tuples[i].sum != 0)
            printf("RowID = %lu\tBucket = %lu\tSum = %lu\n",hist -> hist_tuples[i].payload ,hist -> hist_tuples[i].key,hist -> hist_tuples[i].sum);
        i++;
    }
    printf("----------------------------------------------\n");
}
