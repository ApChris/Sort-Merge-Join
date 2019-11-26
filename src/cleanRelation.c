
#include "../include/cleanRelation.h"

// A fucntion that clears a range of keys in a relation!
void Clean_Relation(relation * rel, int64_t start, int64_t end)
{
    int64_t i = start;
    while(i < end)
    {
        rel -> tuples[i].key = 0;
        for (uint64_t j = 0; j < rel -> tuples[i].position; j++)
        {
            rel -> tuples[i].payload[j] = 0;
        }
        i++;
    }
}
