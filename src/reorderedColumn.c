#include <stdio.h>
#include <stdlib.h>
#include "../include/reorderedColumn.h"

void ReorderedColumn(relation * rel_old, relation * rel_new, psum * ps)
{
    uint64_t i = 0;
    uint64_t selected_byte = 0;
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

        rel_new -> tuples[ps -> psum_tuples[result].position].payload = rel_old -> tuples[i].payload;

        ps -> psum_tuples[result].position++;
        i++;
    }
}
