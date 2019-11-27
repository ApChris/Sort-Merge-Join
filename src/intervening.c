#include "../include/intervening.h"
#include "../include/relation.h"


// intervening_array * intervening_Array_Init()
// {

// 	intervening_array intervening_Array_struct;
//     intervening_array * intervening_Array = &intervening_Array_struct;
// 	if ((intervening_Array = ((intervening_array * )malloc(sizeof(intervening_array)))) == NULL)
// 	{
// 		perror("intervening.c, first malloc");
// 		exit(-1);
// 	}

// 	intervening_Array -> position = 0;
// 	intervening_Array -> payload_array = NULL;

// 	return intervening_Array;
// }

intervening * interveningInit()
{
	intervening intervening_struct;
	intervening * intervening_S = &intervening_struct;
	if ((intervening_S = ((intervening * )malloc(sizeof(intervening)))) == NULL)
	{
		perror("intervening.c, first malloc");
		exit(-1);
	}

	intervening_S -> position = 0;
	intervening_S -> rowId = NULL;
	intervening_S -> final_rel = NULL;

	return intervening_S;
}


// intervening *updateInterveningStruct(intervening *intervening, uint64_t *array_A, uint64_t *array_B){

// }

// void pushJoinedElements_v2(relation * temp_rel, uint64_t * payload_A, uint64_t * payload_B)
// {
// 	// An oxi monadiko payload, push, alliws continue.
// 	if (array -> payload_array == NULL)
// 	{
// 		array -> payload_array = ((intervening_array *)malloc(((array -> position)+1)*sizeof(uint64_t)));
// 		array -> payload_array[array -> position] = payload;
// 		array -> position++;
// 	}
// 	else
// 	{
// 		array -> payload_array = ((intervening_array*)realloc(array -> payload_array, (array -> position+1)*sizeof(uint64_t)));
// 		array -> payload_array[array -> position] = payload;
// 		array -> position++;
// 	}
// }

void pushJoinedElements_v2(relation * temp_rel, uint64_t * payload_A, uint64_t * payload_B, uint64_t counter, uint64_t key){
	if (counter == 0)
	{
		temp_rel->tuples = (tuple*)malloc(sizeof(tuple));
		temp_rel->tuples[counter].position = 2;
		temp_rel->tuples[counter].payload = (uint64_t*)malloc(sizeof(uint64_t)*2);
		temp_rel->tuples[counter].payload[0] = payload_A;
		temp_rel->tuples[counter].payload[1] = payload_B;
		temp_rel->tuples[counter].key = key;
		temp_rel->num_tuples=1;
	}else
	{
		temp_rel->tuples = (tuple*)realloc(temp_rel->tuples, (sizeof(tuple)*(counter+1)));
		temp_rel->tuples[counter].position = 2;
		temp_rel->tuples[counter].payload = (uint64_t*)malloc(sizeof(uint64_t)*2);
		temp_rel->tuples[counter].payload[0] = payload_A;
		temp_rel->tuples[counter].payload[1] = payload_B;
		temp_rel->tuples[counter].key = key;
		temp_rel->num_tuples++;
	}
}


uint64_t Join_v2(intervening * final_interv, relation * rel_A, relation * rel_B, uint64_t rowIdA, uint64_t rowIdB)
{
	// final_interv has intervening results before joining relA and relB.
	// In the end, final_interv will also have the result of the relA=relB

	relation struct_A;
	relation * temp_rel = &struct_A;

	int join_flag = 0;

	uint64_t counter = 0;

	uint64_t mark = 0, a = 0, b = 0;

	while(a < rel_A->num_tuples && b < rel_B->num_tuples)
	{

			while(rel_A->tuples[a].key < rel_B->tuples[b].key)
			{
				a++;
				if(a==rel_A->num_tuples){
					final_interv -> final_rel = temp_rel;
					return counter;
				}

			}
			while(rel_A->tuples[a].key > rel_B->tuples[b].key)
			{
				b++;
				if(b==rel_B->num_tuples){
					final_interv -> final_rel = temp_rel;
					return counter;
				}

			}
			mark = b;


		while (rel_A->tuples[a].key == rel_B->tuples[mark].key)
		{
			b = mark;
			while (rel_A->tuples[a].key == rel_B->tuples[b].key)
			{
				// keyA = keyB
				// head = pushJoinedElements(head, rel_A->tuples[a].key, rel_A->tuples[a].payload, rel_B->tuples[b].payload);
			
				if (join_flag == 0)
				{
					if (final_interv->position == NULL)
					{
						final_interv -> rowId = (uint64_t*)malloc(sizeof(uint64_t)*2);
						final_interv -> rowId[0] = rowIdA;
						final_interv -> rowId[1] = rowIdB;
						final_interv -> position = 2;
						temp_rel = (relation*)malloc(sizeof(relation));
						relation struct_final;
						final_interv -> final_rel = &struct_final;
						printf("mphka sto malloc\n");

					}
					else
					{
						final_interv -> rowId = (uint64_t*)realloc(final_interv->rowId, (sizeof(uint64_t)*((final_interv -> position) + 1)));
						final_interv -> position += 1;
						printf("mphka sto realloc\n");
					}
					join_flag = 1;

				}

				// push
				
				pushJoinedElements_v2(temp_rel, rel_A->tuples[a].payload, rel_B->tuples[b].payload, counter,
					rel_A->tuples[a].key);
				
				// final_interv -> final_rel -> num_tuples = counter;
				
				if(b==rel_B->num_tuples)
				{
					final_interv -> final_rel = temp_rel;
					return counter;
				}
				b++;
				counter++;
			}
			a++;
			if(a==rel_A->num_tuples)
			{
				final_interv -> final_rel = temp_rel;
				return counter;
			}
		}

	}
	return counter;
}



// uint64_t binarySearch(uint64_t * array,uint64_t start, uint64_t end, uint64_t payload)
// {
// 	if(end >= start)
// 	{
// 		uint64_t mid = start + (end - start) /2;

// 		if(array[mid] == payload)
// 		{
// 			return mid;
// 		}

// 		if(array[mid] > payload)
// 		{
// 			return binarySearch(array, start, mid - 1, payload);
// 		}

// 		return binarySearch(array, mid + 1, end, payload);
// 	}
// 	return -1;
// }


// uint64_t Sum_Column(intervening_array * array)
// {
// 	uint64_t sum = 0;
// 	for (size_t i = 0; i < array -> position; i++)
// 	{
// 		sum += array -> payload_array[i];
// 	}
// 	return sum;
// }

// intervening * Update(intervening * inter, metadata * meta, intervening_array * array)
// {
//
// }
