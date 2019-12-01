#ifndef _PROCESSRELATION_H
#define _PROCESSRELATION_H

#include <stdlib.h>
#include "splitBucket.h"
#include "quicksort.h"
#include "reorderedColumn.h"
#include "metadata.h"

void ProcessRelation(relation * rel_old, histogram * hist, psum * ps, relation * rel_new,int64_t sel_byte);
void Print_Relation(relation * rel, histogram * hist, psum * ps);
//void Radix_Sort(relation * rel, relation * rel_final);
relation * Radix_Sort(relation * rel, relation * rel_final);



void Create_Relation(metadata * md, uint64_t md_pos,uint64_t array_pos, relation * rel);
void Print_Relation_2(relation * rel);
void Update_Tuple_Payload(metadata * md, relation * rel, uint64_t pos, uint64_t key, uint64_t payload);
relation * Filter(relation * rel, uint64_t limit, char symbol);
void Update_Relation_Keys(metadata * md, uint64_t md_row, uint64_t md_column, relation * rel, uint64_t pos);
void CheckSum(metadata * md, uint64_t md_row, uint64_t md_column, relation * rel, uint64_t pos);
#endif
