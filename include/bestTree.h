#ifndef _BESTTREE_H_
#define _BESTTREE_H_

#include "statistics.h"
#include <stdbool.h>
typedef struct best_tree
{
	query_tuple * qt_best_tree;
    uint64_t num_predicates;
	uint64_t active_bits;
	double cost;
	statistics * tree_stats;
}best_tree;

typedef struct bestTree
{
	double cost;
	uint64_t path;
}bestTree;

void Init_BestTree(best_tree*** bt, uint64_t num_relations);
bestTree * Join_Enum(statistics * query_stats, work_line * wl_ptr, uint64_t pos);
double Calculate_Cost(statistics * query_stats, uint64_t file1_ID, uint64_t file1_column, uint64_t file2_ID, uint64_t file2_column, double cost);
bestTree * Best_Tree_Init();
void Best_Tree_Update(bestTree * bt, uint64_t path, double cost);
#endif
