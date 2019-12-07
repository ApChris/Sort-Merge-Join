#ifndef _PROCESSRELATION_H
#define _PROCESSRELATION_H

#define TAG 1400322

#include <stdlib.h>
#include "splitBucket.h"
#include "quicksort.h"
#include "reorderedColumn.h"
#include "metadata.h"
#include "work.h"
#include "intervening.h"

void ProcessRelation(relation * rel_old, histogram * hist, psum * ps, relation * rel_new,int64_t sel_byte);
void Print_Relation(relation * rel, histogram * hist, psum * ps);
//relation * Radix_Sort(relation * rel, relation * rel_final);
relation * Radix_Sort(relation * rel);

relation * Init_pointer();
uint64_t Find_Query_Tuple(query_tuple * qt, uint64_t file_ID, uint64_t file_column,uint64_t counter);

//void Create_Relation(metadata * md, uint64_t md_pos,uint64_t array_pos, relation * rel);
relation * Create_Relation(metadata * md, uint64_t md_pos,uint64_t array_pos);
void Print_Relation_2(relation * rel);
void Update_Tuple_Payload(metadata * md, relation * rel, uint64_t pos, uint64_t key, uint64_t payload);
relation * Filter(relation * rel, uint64_t limit, char symbol);
void Update_Relation_Keys(metadata * md, uint64_t md_row, uint64_t md_column, relation * rel, uint64_t pos);
uint64_t CheckSum(metadata * md, uint64_t md_row, uint64_t md_column, relation * rel, uint64_t pos);
void Execute_Queries(metadata * md, work_line * wl_ptr);
#endif
