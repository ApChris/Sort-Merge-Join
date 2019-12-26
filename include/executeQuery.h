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

// relation * UpdateRel(metadata * md, uint64_t md_pos,uint64_t array_pos, intervening * interv_final,uint64_t interv_pos, uint64_t payload_pos);

void Execute_Queries(metadata * md, work_line * wl_ptr, uint64_t query);
void Print_Available_Filters(query_tuple * qt_filters, uint64_t filter_counter);
void Free_Execute_Memory(query_tuple * qt_filters, query_tuple * qt_predicates, intervening *interv_final,
	uint64_t filter_counter, uint64_t predicate_counter);
void Show_Results(uint64_t null_flag, work_line * wl_ptr, intervening * interv_final, metadata * md, uint64_t i);

#endif
