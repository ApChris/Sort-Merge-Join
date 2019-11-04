
#include "../include/cleanRelation.h"

// A fucntion that clears a range of keys in a relation!
void Clean_Relation(relation * rel, int64_t start, int64_t end)
{
    int64_t i = start;
    while(i < end)
    {
        rel -> tuples[i].key = 0;
        rel -> tuples[i].payload = i;
        i++;
    }
}
