#ifndef _PROCESSRELATION_H
#define _PROCESSRELATION_H

#include <stdlib.h>
#include "splitBucket.h"
#include "quicksort.h"
#include "reorderedColumn.h"
#include "metadata.h"

void ProcessRelation(relation * rel_old, histogram * hist, psum * ps, relation * rel_new,int64_t sel_byte);
void Print_Relation(relation * rel, histogram * hist, psum * ps);
void Final_Relation(relation * rel, relation * rel_final);

void Create_Relation(metadata * md, uint64_t md_pos,uint64_t array_pos, relation * rel);
void Filter(relation * rel, uint64_t limit, char symbol, relation * rel_final);

#endif
