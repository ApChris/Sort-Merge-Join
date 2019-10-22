#ifndef _HISTOGRAM_H
#define _HISTOGRAM_H

#include <stdint.h>

typedef struct histogram
{
    uint64_t key;
    uint64_t payload;
    uint64_t sum;
}relation;


#endif
