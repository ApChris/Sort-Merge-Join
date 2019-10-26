#ifndef _PROCESSRELATION_H
#define _PROCESSRELATION_H

#include "splitBucket.h"

void ProcessRelation(relation * rel_old, histogram * hist, psum * ps, relation * rel_new);
void Print_Relation(relation * rel, histogram * hist, psum * ps);


#endif
