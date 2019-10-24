#ifndef _SPLITBUCKET_H
#define _SPLITBUCKET_H


#include "../include/psum.h"

void Split_Bucket(relation * rel_old, histogram * hist, psum *ps, relation *rel_new, uint64_t start, uint64_t end);

#endif
