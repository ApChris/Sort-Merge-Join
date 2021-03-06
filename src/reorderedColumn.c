#include <stdio.h>
#include <stdlib.h>
#include "../include/reorderedColumn.h"

void ReorderedColumn(relation * rel_old, relation * rel_new, psum * ps)
{
    uint64_t i = 0;
    //  = 7 because we need the 8th byte(0 = 1st byte from the right, 1 = 2nd byte from the right)
    uint64_t selected_byte = 7;
    uint64_t result = 0;

    if((rel_new -> tuples = (tuple *)malloc(rel_old -> num_tuples * sizeof(tuple))) == NULL)
    {
        perror("reorderedColumn.c , first malloc\n");
        exit(-1);
    }

    rel_new -> num_tuples = rel_old -> num_tuples;

    while(i < rel_old -> num_tuples)
    {

        result = (rel_old -> tuples[i].key >> (8*selected_byte) & 0xFF);

        //  ps -> psum_tuples[result].position -> it means the position that we are going to write
        rel_new -> tuples[ps -> psum_tuples[result].position].key = rel_old -> tuples[i].key;

        //if(rel_old -> tuples[i].position)
        if((rel_new -> tuples[ps -> psum_tuples[result].position].payload = (uint64_t *)malloc((rel_old -> tuples[i].position) *sizeof(uint64_t))) == NULL)
        {
            perror("reordered.c , 2nd malloc\n");
            exit(-1);
        }
        for (size_t j = 0; j < rel_old -> tuples[i].position; j++)
        {
            rel_new -> tuples[ps -> psum_tuples[result].position].payload[j] = rel_old -> tuples[i].payload[j];// payload
        }

        rel_new -> tuples[ps -> psum_tuples[result].position].position = rel_old -> tuples[i].position;
        ps -> psum_tuples[result].position++;
        i++;
    }
}
