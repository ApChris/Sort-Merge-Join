
#include "../include/result.h"
#include "../include/relation.h"




result * resultInit()
{
	result * head_result;
	if((head_result = (result*)malloc(sizeof(result))) == NULL)
	{
		perror("result.c, first malloc");
		exit(-1);
	}
	head_result->num_results = 0;
	head_result->next_result = NULL;

	return head_result;
}

void insertNewResult(result *res)
{
	result * current_result = resultInit();
	res->next_result = current_result;
}

void pushJoinedElements(result *res, uint64_t key, uint64_t payload_A, uint64_t payload_B){
	result *current_result = res;
	result *previous_result = res;
	int flag = 0;

	//printf("Elements to be joined: key %lu\tpayload_A: %lu\tpayload_B: %lu\n", key, payload_A, payload_B);

	while(current_result != NULL)
	{
		//	bucket hasn't reached 1 MB, so push it
		if(current_result->num_results < ROWS)
		{
			current_result->buffer[current_result->num_results][0] = key;
			current_result->buffer[current_result->num_results][1] = payload_A;
			current_result->buffer[current_result->num_results][2] = payload_B;

			current_result->num_results++;
			flag=1;
		}

		previous_result = current_result;
		current_result = current_result->next_result;
	}

	//	bucket has reached 1 MB, create a new Result and push it there
	if(flag == 0)
	{
		insertNewResult(previous_result);
		current_result = previous_result->next_result;

		current_result->buffer[current_result->num_results][0] = key;
		current_result->buffer[current_result->num_results][1] = payload_A;
		current_result->buffer[current_result->num_results][2] = payload_B;

		current_result->num_results++;
	}
}

void printResult(result *res)
{
	result *current_result = res;

	while(current_result != NULL)
	{
		for (uint64_t i = 0; i < current_result->num_results; i++)
		{
			printf("%lu) key: %lu\tpayload_A: %lu\t\tpayload_B: %lu\n",i, current_result->buffer[i][0],current_result->buffer[i][1], current_result->buffer[i][2]);
		}
		current_result = current_result->next_result;
	}
}

void Join(relation *rel_A, uint64_t start_A, uint64_t end_A, relation *rel_B, uint64_t start_B, uint64_t end_B, uint64_t sel_byte, result *res)
{

	uint64_t result = (rel_A -> tuples[start_A].key >> (8*sel_byte) & 0xFF);

	uint64_t mark = start_B, a = start_A, b = start_B;

	printf("\n\n\nkey A[%lu]:%lu  bucket:%lu ----- startA:%lu - endA: %lu ------>>>> key B[%lu]:%lu startB:%lu - endB:%lu ---- Max splits of this bucket = %lu\n",start_A,rel_A -> tuples[start_A].key,result, start_A, end_A,start_B,rel_B -> tuples[start_B].key, start_B, end_B,sel_byte);
	while(mark < end_A && mark < end_B)
	{
		if(mark == start_A)
		{
			while(rel_A->tuples[a].key < rel_B->tuples[b].key)
			{
				a++;
				if(a == end_A)
				{
					return;
				}
			}
			while(rel_A->tuples[a].key > rel_B->tuples[b].key)
			{
				b++;
				if(b == end_B)
				{
					return;
				}
			}
			mark = b;
		}
		if (rel_A->tuples[a].key == rel_B->tuples[b].key)
		{
			//printf("Element to be pushed\n");
			//printf("key %lu\tpayload_A: %lu\tpayload_B: %lu\n", rel_A->tuples[a].key, rel_A->tuples[a].payload, rel_B->tuples[b].payload);
			pushJoinedElements(res, rel_A->tuples[a].key, rel_A->tuples[a].payload, rel_B->tuples[b].payload);
			//printf("Element pushed successfully\n");

			//printf("\n");
			b++;
			if(b == end_B)
			{
				return;
			}
		}
		else
		{
			b = mark;
			a++;
			if(a == end_A)
			{
				return;
			}
			mark = start_A;	//	reset
		}
	}
}
