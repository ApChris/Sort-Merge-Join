#include "../include/intervening.h"
#include "../include/relation.h"


intervening_array * intervening_Array_Init()
{

	intervening_array intervening_Array_struct;
    intervening_array * intervening_Array = &intervening_Array_struct;
	if ((intervening_Array = ((intervening_array * )malloc(sizeof(intervening_array)))) == NULL)
	{
		perror("intervening.c, first malloc");
		exit(-1);
	}

	intervening_Array -> position = 0;
	intervening_Array -> payload_array = NULL;

	return intervening_Array;
}

intervening * intervening_Init()
{


	intervening intervening_struct;
	intervening * intervening_S = &intervening_struct;
	if ((intervening_S = ((intervening * )malloc(sizeof(intervening)))) == NULL)
	{
		perror("intervening.c, first malloc");
		exit(-1);
	}

	intervening_S -> total_arrays = 0;
	intervening_S -> rowId = NULL;

	return intervening_S;
}


// intervening *updateInterveningStruct(intervening *intervening, uint64_t *array_A, uint64_t *array_B){

// }

void pushJoinedElements_v2(uint64_t payload, intervening_array * array)
{
	// An oxi monadiko payload, push, alliws continue.
	if (array -> payload_array == NULL)
	{
		array -> payload_array = ((intervening_array *)malloc(((array -> position)+1)*sizeof(uint64_t)));
		array -> payload_array[array -> position] = payload;
		array -> position++;
	}
	else
	{
		array -> payload_array = ((intervening_array*)realloc(array -> payload_array, (array -> position+1)*sizeof(uint64_t)));
		array -> payload_array[array -> position] = payload;
		array -> position++;
	}
}


uint64_t Join_v2(intervening *intervening, relation *rel_A, relation *rel_B, intervening_array *array_A, intervening_array *array_B)
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
				// head = pushJoinedElements(head, rel_A->tuples[a].key, rel_A->tuples[a].payload, rel_B->tuples[b].payload);
				// yparxoun koina keys, opote push payloadA kai push payloadB
				if(array_A -> position > 0)
				{
					if(binarySearch(array_A,0, array_A -> position,rel_A->tuples[a].payload) == -1)
					{
						printf("Den Yparxei\n");
						pushJoinedElements_v2(rel_A->tuples[a].payload, array_A);
					}
					else
					{

						printf("Yparxei\n");
					}
				}
				else
				{
					pushJoinedElements_v2(rel_A->tuples[a].payload, array_A);
				}
				// if(binarySearch(array_B,0, array_B->position,rel_B->tuples[b].payload) == -1)
				// {
				// 	printf("Den Yparxei\n");
				// 	pushJoinedElements_v2(rel_B->tuples[b].payload, array_B);
				// }
				// else
				// {
				//
				// 	printf("Yparxei\n");
				// }
				//pushJoinedElements_v2(rel_A->tuples[a].payload, array_A);
				//pushJoinedElements_v2(rel_B->tuples[b].payload, array_B);
				//printf("key = %lu\tpayload_A = %lu\tpayload_B = %lu\n",rel_A->tuples[a].key,rel_B->tuples[b].payload,rel_B->tuples[b].payload);
				if(b==rel_B->num_tuples)
				{
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



uint64_t binarySearch(uint64_t * array,uint64_t start, uint64_t end, uint64_t payload)
{
	if(end >= start)
	{
		uint64_t mid = start + (end - start) /2;

		if(array[mid] == payload)
		{
			return mid;
		}

		if(array[mid] > payload)
		{
			return binarySearch(array, start, mid - 1, payload);
		}

		return binarySearch(array, mid + 1, end, payload);
	}
	return -1;
}


uint64_t Sum_Column(intervening_array * array)
{
	uint64_t sum = 0;
	for (size_t i = 0; i < array -> position; i++)
	{
		sum += array -> payload_array[i];
	}
	return sum;
}

// intervening * Update(intervening * inter, metadata * meta, intervening_array * array)
// {
//
// }
