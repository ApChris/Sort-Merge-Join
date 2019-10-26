#include <stdio.h>
#include "../include/splitBucket.h"

void Split_Bucket(relation * rel_old, histogram * hist, psum *ps, relation *rel_new, int64_t start, int64_t end, uint64_t sel_byte)
{
    Clean_Relation(rel_old);

    for(int64_t i = start; i < end; i++)
    {
        rel_old -> tuples[i].key = rel_new -> tuples[i].key;
        rel_old -> tuples[i].payload = rel_new -> tuples[i].payload;
    }
    rel_old -> num_tuples = end;
    Histogram(rel_old,hist,1);
    Print_Histogram(hist);

    Psum(hist,ps);
    Print_Psum(hist,ps);

    //rel_new -> num_tuples = rel_old -> num_tuples;
    uint64_t i = 0;
    int64_t selected_byte = sel_byte;
    uint64_t result = 0;

    while(i < rel_old -> num_tuples)
    {
        result = (rel_old -> tuples[i].key >> (8*selected_byte) & 0xFF);

        //  ps -> psum_tuples[result].position -> it means the position that we are going to write
        rel_new -> tuples[ps -> psum_tuples[result].position].key = rel_old -> tuples[i].key;

        rel_new -> tuples[ps -> psum_tuples[result].position].payload = rel_old -> tuples[i].payload;

        ps -> psum_tuples[result].position++;
        i++;
    }

    RestorePsum(hist, ps);  //psum's position returns to its initial values

    printf("start: %ld, end: %ld\n", start, end);

    rel_old -> num_tuples = rel_new -> num_tuples;

}
