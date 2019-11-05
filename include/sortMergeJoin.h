#ifndef _SORT_MERGE_JOIN_H_
#define _SORT_MERGE_JOIN_H_


#include "relation.h"
#include "result.h"
#include <time.h>

result * SortMergeJoin(relation * relation_A_final, relation * relation_B_final);

#endif
