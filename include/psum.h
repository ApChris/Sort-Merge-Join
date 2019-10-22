#ifndef _PSUM_H
#define _PSUM_H

#include <stdint.h>

typedef struct psum
{
    uint64_t key;
    uint64_t payload;
    uint64_t position;
}psum;


#endif
