#ifndef _WORK_H_
#define _WORK_H_

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>

typedef struct parameter_tuple
{
    uint64_t file1_ID;
}parameter_tuple;

typedef struct parameter_rel
{
    parameter_tuple * tuples;
    uint64_t num_tuples;
}parameter_rel;

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
    parameter_rel * parameters;
    uint64_t num_parameters;

    predicate_rel * predicates;
    uint64_t num_predicates;

    filter_rel * filters;
    uint64_t num_filters;

    select_rel * selects;
    uint64_t num_selects;

}work_line;


void Push_Predicates(work_line * wl_ptr, uint64_t file1_ID, uint64_t file1_column, uint64_t file2_ID, uint64_t file2_column, uint64_t counter);
void PredicateRelInit(work_line * wl_ptr);

void FiltersRelInit(work_line * wl_ptr);
void Push_Filters(work_line * wl_ptr, uint64_t file1_ID, uint64_t file1_column, char symbol, uint64_t limit, uint64_t counter);

void SelectsRelInit(work_line * wl_ptr);
void Push_Selects(work_line * wl_ptr, uint64_t file1_ID, uint64_t file1_column, uint64_t counter);

void ParametersRelInit(work_line * wl_ptr);
void Push_Parameters(work_line * wl_ptr, uint64_t file1_ID, uint64_t counter);

work_line * WorkLineInit();
#endif
