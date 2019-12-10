#ifndef _EXECUTE_QUERY_H_
#define _EXECUTE_QUERY_H_

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include "relation.h"
#include "metadata.h"
#include "intervening.h"
#include "work.h"
#include "processRelation.h"

uint64_t Find_Query_Tuple(query_tuple * qt, uint64_t file_ID,uint64_t counter);
uint64_t CheckSum(metadata * md, uint64_t md_row, uint64_t md_column, relation * rel, uint64_t pos);
relation * Init_pointer();

query_tuple * Init_Query_Tuple();

relation * Update_Relation(metadata * md, uint64_t md_pos,uint64_t array_pos, intervening * interv_final,uint64_t interv_pos, uint64_t payload_pos);

void Execute_Queries(metadata * md, work_line * wl_ptr);


#endif
