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
#include "statistics.h"
#include "jobScheduler.h"


uint64_t Find_Query_Tuple(query_tuple * qt, uint64_t file_ID,uint64_t counter);
uint64_t CheckSum(metadata * md, uint64_t md_row, uint64_t md_column, relation * rel, uint64_t pos);
relation * Init_pointer();

query_tuple * Init_Query_Tuple();

void Execute_Queries(metadata * md, work_line * wl_ptr,uint64_t query, statistics * stats, char c, job_scheduler * scheduler);
void Print_Available_Filters(query_tuple * qt_filters, uint64_t filter_counter);
uint64_t Current_Best_Predicate(uint64_t num_predicates, uint64_t pred, uint64_t j);
bool Check_Q(uint64_t i, char c);
#endif
