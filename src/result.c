#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include "../include/result.h"
#include "../include/relation.h"



result * resultInit(){
	result *head_result;
	if((head_result = (result*)malloc(sizeof(result))) == NULL){
		perror("result.c, first malloc");
		exit(-1);
	}
	head_result->num_results = 0;
	head_result->next_result = NULL;

	return head_result;
}

void insertNewResult(result *res){
	result *current_result = resultInit();
	// if((current_result = (result*)malloc(sizeof(result))) == NULL){
	// 	perror("result.c, insertNewResult mallon");
	// 	exit(-1);
	// }
	// current_result->num_results = 0;

	res->next_result = current_result;
}

void pushJoinedElements(result *res, uint64_t key, uint64_t rowIdS, uint64_t rowIdR){
	result *current_result = res;
	result *previous_result = res;
	int flag = 0;

	while(current_result != NULL)
	{
		//	bucket hasn't reached 1 MB, so push it
		if(current_result->num_results < ROWS){
			current_result->buffer[current_result->num_results][0] = key;
			current_result->buffer[current_result->num_results][1] = rowIdS;
			current_result->buffer[current_result->num_results][2] = rowIdR;

			current_result->num_results++;
			flag=1;
		}

		previous_result = current_result;
		current_result = current_result->next_result;
	}

	//	bucket has reached 1 MB, create a new Result and push it there
	if(flag == 0){
		insertNewResult(previous_result);
		current_result = previous_result->next_result;

		current_result->buffer[current_result->num_results][0] = key;
		current_result->buffer[current_result->num_results][1] = rowIdS;
		current_result->buffer[current_result->num_results][2] = rowIdR;

		current_result->num_results++;
	}
}

void printResult(result *res){
	result *current_result = res;

	while(current_result != NULL){
		for (uint64_t i = 0; i < current_result->num_results; i++)
		{
			printf("key: %ld\trowIdR: %ld\trowIdS: %ld\n", current_result->buffer[i][0], 
				current_result->buffer[i][1], current_result->buffer[i][2]);
		}
		current_result = current_result->next_result;
	}
}

void Join(relation *relation_R, relation *relation_S, result *res){
	uint64_t mark, r, s = 0;
	while(mark < relation_R->num_tuples){

		while(relation_R->tuples[r].key < relation_S->tuples[s].key){
			r++;
		}
		while(relation_R->tuples[r].key > relation_S->tuples[s].key){
			s++;
		}
		mark = s;

		if (relation_R->tuples[r].key == relation_S->tuples[s].key)
		{
			pushJoinedElements(res, relation_R->tuples[r].key, relation_R->tuples[r].payload
				, relation_S->tuples[s].payload);
			s++;
		}
		else
		{
			s = mark;
			r++;
			mark = 0;	//	reset
		}
	}
}