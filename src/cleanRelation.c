
#include "../include/cleanRelation.h"

void Clean_Relation(relation * rel)
{
    uint64_t i = 0;
    while(i < rel -> num_tuples)
    {
        rel -> tuples[i].key = 0;
        rel -> tuples[i].payload = i;
        i++;
    }
}
