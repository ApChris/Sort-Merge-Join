#include <stdio.h>
#include "../include/splitBucket.h"

void Split_Bucket(relation * rel_old, histogram * hist, psum *ps, relation *rel_new, int64_t start, int64_t end, uint64_t sel_byte)
{
    Clean_Relation(rel_old,start,end);


//
    for(int64_t i = start; i < end; i++)
    {
        rel_old -> tuples[i].key = rel_new -> tuples[i].key;
        rel_old -> tuples[i].payload = rel_new -> tuples[i].payload;
    }
//     rel_old -> num_tuples = end;
    histogram struct_h2;
    histogram *hist2 = &struct_h2;
    Histogram(rel_old,hist2,sel_byte,start,end);
    Print_Histogram(hist2);
//
    psum struct_p2;
    psum *ps2 = &struct_p2;
    Psum(hist2,ps2,start);
     Print_Psum(hist2,ps2);
//
//     //rel_new -> num_tuples = rel_old -> num_tuples;
     uint64_t i = start;
//     int64_t selected_byte = sel_byte;
    uint64_t result = 0;
//
    while(i < end)
    {
// //        printf(" i = %ld\n",i);
        result = (rel_old -> tuples[i].key >> (8*sel_byte) & 0xFF);
//
//         //  ps -> psum_tuples[result].position -> it means the position that we are going to write
        rel_new -> tuples[ps2 -> psum_tuples[result].position].key = rel_old -> tuples[i].key;
//
        rel_new -> tuples[ps2 -> psum_tuples[result].position].payload = rel_old -> tuples[i].payload;
//
        ps2 -> psum_tuples[result].position++;
        i++;
    }
//
    RestorePsum(hist2, ps2);  //psum's position returns to its initial values
//
//     //printf("start: %ld, end: %ld\n", start, end);
//
//     rel_old -> num_tuples = rel_new -> num_tuples;
//    printf("END\n");
}
