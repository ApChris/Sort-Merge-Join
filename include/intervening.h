#ifndef _INTERVENING_H
#define _INTERVENING_H

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include "relation.h"
#include "processRelation.h"

typedef struct intervening
{
	uint64_t position;
	uint64_t * rowId;
	relation * final_rel;
}intervening;


intervening * interveningInit();

uint64_t Join_v2(intervening * final_interv, relation * rel_A, relation * rel_B, uint64_t rowIdA, uint64_t rowIdB);
void pushJoinedElements_v2(relation * temp_rel, relation * relation_A, relation * relation_B,uint64_t posA, uint64_t posB, uint64_t counter, uint64_t key);
uint64_t FindRowID(intervening * final_interv, uint64_t rowID);

void pushJoinedElements_Self(relation * temp_rel, relation * relation_A, uint64_t posA, uint64_t counter, uint64_t key);
uint64_t Self_Join(intervening * final_interv, relation * rel_A, relation * rel_B, uint64_t rowIdA, uint64_t rowIdB);
relation * Scan(relation * rel_A, relation * rel_B);
#endif
