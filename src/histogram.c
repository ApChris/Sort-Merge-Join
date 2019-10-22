#include <stdio.h>
#include <stdlib.h>
#include "../include/histogram.h"


void Histogram(relation * rel, histogram * hist)
{

    uint64_t i = 0;
    uint64_t selected_byte = 0;
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

    i = 0;
    while(i < rel -> num_tuples)
    {
        result = (rel -> tuples[i].key >> (8*selected_byte) & 0xFF);
        printf("%ld - first byte: %ld \n", rel -> tuples[i].key, result);

        hist -> hist_tuples[result].sum++;
        i++;
    }

}


void Print_Histogram(histogram * hist)
{
    uint64_t i = 0;
    while(i < hist -> num_tuples)
    {
        printf("RowID = %ld --- Bucket  = %ld --- Sum = %ld\n",hist -> hist_tuples[i].payload ,hist -> hist_tuples[i].key,    hist -> hist_tuples[i].sum);
        i++;
    }
}
