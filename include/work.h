#ifndef _WORK_H_
#define _WORK_H_

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>



typedef struct predicate_tuple
{
    uint64_t file1_ID;
    uint64_t file1_column;
    uint64_t file2_ID;
    uint64_t file2_column;
}predicate_tuple;

typedef struct predicate_rel
{
    predicate_tuple * tuples;
    uint64_t num_tuples;
}predicate_rel;

typedef struct filter_tuple
{
    uint64_t file1_ID;
    uint64_t file1_column;
    char symbol;
    uint64_t limit;
}filter_tuple;

typedef struct filter_rel
{
    filter_tuple * tuples;
    uint64_t num_tuples;
}filter_rel;

typedef struct select_tuple
{
    uint64_t file1_ID;
    uint64_t file1_column;
}select_tuple;

typedef struct select_rel
{
    select_tuple * tuples;
    uint64_t num_tuples;
}select_rel;

typedef struct work_line
{
    uint64_t * parameters;

    predicate_rel * predicates;
    uint64_t num_predicates;

    filter_rel * filters;
    select_rel * selects;

}work_line;


void Push_Predicates(work_line * wl_ptr, uint64_t file1_ID, uint64_t file1_column, uint64_t file2_ID, uint64_t file2_column, uint64_t counter);
void PredicateRelInit(work_line * wl_ptr);
work_line * WorkLineInit();
#endif
