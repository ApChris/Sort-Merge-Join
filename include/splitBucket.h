#ifndef _SPLITBUCKET_H
#define _SPLITBUCKET_H


#include "psum.h"
#include "cleanRelation.h"

// this function creates a new histogram/psum and set keys at correct position
void Split_Bucket(relation * rel_old, histogram * hist, psum *ps, relation *rel_new, int64_t start, int64_t end, uint64_t sel_byte);
void printBucket(relation * rel, int64_t start, int64_t end);
#endif
