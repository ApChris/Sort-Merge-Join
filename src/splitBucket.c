#include <stdio.h>
#include "../include/splitBucket.h"

void Split_Bucket(relation * rel_old, histogram * hist, psum *ps, relation *rel_new, int64_t start, int64_t end, uint64_t sel_byte)
{
    // Clean the area of the relation
    Clean_Relation(rel_old,start,end);

    for(int64_t i = start; i < end; i++)
    {
        rel_old -> tuples[i].key = rel_new -> tuples[i].key;
        rel_old -> tuples[i].payload = rel_new -> tuples[i].payload;
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

        //  ps -> psum_tuples[result].position -> it means the position that we are going to write
        rel_new -> tuples[ps -> psum_tuples[result].position].key = rel_old -> tuples[i].key;

        rel_new -> tuples[ps -> psum_tuples[result].position].payload = rel_old -> tuples[i].payload;

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
		printf("%lu, %lu\n", rel -> tuples[i].key, rel -> tuples[i].payload);
	}
}
