#ifndef _TUPLE_H
#define _TUPLE_H

#include <stdint.h>

// pair of key, payload
typedef struct tuple
{
    uint64_t key;
    uint64_t * payload;
    uint64_t position;
}tuple;


#endif
