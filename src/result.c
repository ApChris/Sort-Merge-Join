
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

result * pushJoinedElements(result *head, uint64_t key, uint64_t * payload_A, uint64_t * payload_B)
{
	result *current_result = head;
	result *previous_result = head;

	//	bucket hasn't reached 1 MB, so push it
	if(current_result->num_results < ROWS)
	{
		current_result->buffer[current_result->num_results][0] = key;
		current_result->buffer[current_result->num_results][1] = payload_A[0];
		current_result->buffer[current_result->num_results][2] = payload_B[0];

		current_result->num_results++;
	}
	else
	{
		//	bucket has reached 1 MB, create a new Result and push it there
		previous_result = current_result;
		insertNewResult(previous_result);
		current_result = previous_result->next_result;

		current_result->buffer[current_result->num_results][0] = key;
		current_result->buffer[current_result->num_results][1] = payload_A[0];
		current_result->buffer[current_result->num_results][2] = payload_B[0];

		current_result->num_results++;

	}

	return current_result;
}

void printResult(result *res)
{
	result *current_result = res;
	uint64_t j=0;

	while(current_result != NULL)
	{
		for (uint64_t i = 0; i < ROWS; i++)
		{
			if (current_result->buffer[i][0] == 0 && current_result->buffer[i][1] == 0 && current_result->buffer[i][2] == 0)
			{
				break;
			}
			printf("%lu) key: %lu\tpayload_A: %lu\t\tpayload_B: %lu\n",(ROWS*j)+i, current_result->buffer[i][0],current_result->buffer[i][1], current_result->buffer[i][2]);
		}
		current_result = current_result->next_result;
		j++;
	}
}


void Deallocate_List(result *res)
{
	result *current_result;

	while(res != NULL)
	{
		current_result = res;
		res = res->next_result;
		free(current_result);
	}
}

uint64_t Join(relation *rel_A, relation *rel_B, result *head)
{
	uint64_t counter = 0;

	uint64_t mark = 0, a = 0, b = 0;

	while(a < rel_A->num_tuples && b < rel_B->num_tuples)
	{

			while(rel_A->tuples[a].key < rel_B->tuples[b].key)
			{
				a++;
				if(a==rel_A->num_tuples){
					return counter;
				}

			}
			while(rel_A->tuples[a].key > rel_B->tuples[b].key)
			{
				b++;
				if(b==rel_B->num_tuples){
					return counter;
				}

			}
			mark = b;


		while (rel_A->tuples[a].key == rel_B->tuples[mark].key)
		{
			b = mark;
			while (rel_A->tuples[a].key == rel_B->tuples[b].key)
			{
				head = pushJoinedElements(head, rel_A->tuples[a].key, rel_A->tuples[a].payload, rel_B->tuples[b].payload);
				if(b==rel_B->num_tuples){
					return counter;
				}
				b++;
				counter++;
			}
			a++;
			if(a==rel_A->num_tuples)
			{
				return counter;
			}
		}

	}
	return counter;
}


resultBucket * resultBucket_Init(uint64_t totalQueries)
{
	resultBucket * result;
	if((result = (resultBucket *)malloc(totalQueries * sizeof(resultBucket))) == NULL)
	{
		perror("result.c, first malloc");
		exit(-1);
	}
	for (size_t i = 0; i < totalQueries; i++)
	{
		result[i].id = 0;
		result[i].num_payloads = 0;
		result[i].is_null = false;
		result[i].payload = NULL;
	}
	return result;
}

void resultBucket_Push(resultBucket * result ,uint64_t * array_payloads,bool is_null, uint64_t pos,uint64_t num_payloads)
{

	// if current query has uint64_t results
	if(is_null == true)
	{
		if((result[pos].payload = (uint64_t *)malloc(num_payloads * sizeof(uint64_t))) == NULL)
		{
			perror("resultBucket_Push.c , first malloc\n");
			exit(-1);
		}

		for (size_t i = 0; i < num_payloads; i++)
		{
			result[pos].payload[i] = array_payloads[i];
		}
		result[pos].is_null = true;
	}
	// if results are null
	else
	{
		result[pos].is_null = false;
	}
	result[pos].num_payloads = num_payloads;
	result[pos].id = pos;
}
