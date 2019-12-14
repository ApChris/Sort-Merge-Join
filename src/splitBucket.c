#include <stdio.h>
#include "../include/splitBucket.h"

void Split_Bucket(relation * rel_old, histogram * hist, psum *ps, relation *rel_new, int64_t start, int64_t end, uint64_t sel_byte)
{
    // Clean the area of the relation

    for(int64_t i = start; i < end; i++)
    {
        swap(&(rel_old -> tuples[i]),&(rel_new -> tuples[i]));
    }

    // Create a new histogram in range start:end
    Histogram(rel_old,hist,sel_byte,start,end);

    // Create a new psum
    Psum(hist,ps,start);

    int64_t i = start;
    uint64_t result = 0;

    // Get old keys and write them correclty at new relation
    while(i < end)
    {

        result = (rel_old -> tuples[i].key >> (8*sel_byte) & 0xFF);

        swap(&rel_new -> tuples[ps -> psum_tuples[result].position],&rel_old -> tuples[i]);

        ps -> psum_tuples[result].position++;
        i++;
    }

    RestorePsum(hist, ps);  //psum's position returns to its initial values

}

// Print a specific bucket
void printBucket(relation * rel, int64_t start, int64_t end)
{
	for(int64_t i = start; i < end; i++)
	{
        for (size_t j = 0; j < rel -> tuples[i].position; j++) {
            /* code */
            printf("%lu, %lu\n", rel -> tuples[i].key, rel -> tuples[i].payload[j]);

        }
	}
}
