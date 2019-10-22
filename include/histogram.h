#ifndef _HISTOGRAM_H
#define _HISTOGRAM_H

#include <stdint.h>

typedef struct histogram
{
    int64_t key;
    int64_t payload;
    int64_t sum;
}relation;


#endif
