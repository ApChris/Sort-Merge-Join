#ifndef _RELATION_H
#define _RELATION_H

#include "tuple.h"

typedef struct relation
{
    tuple * tuples;
    uint64_t num_tuples;
}relation;


#endif
