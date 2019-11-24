#include "../include/intervening.h"
#include "../include/relation.h"


intervening_array *interveningArrayInit(){
	intervening_array *current;
	if ((current = ((intervening_array*)malloc(sizeof(intervening_array)))) == NULL)
	{
		perror("intervening.c, first malloc");
		exit(-1);
	}

	current->position = 0;
	current->payload_array = NULL;

	return current;
}

intervening_struct *interveningStructInit(){
	intervening_struct *current;
	if ((current = ((intervening_struct*)malloc(sizeof(intervening_struct)))) == NULL)
	{
		perror("intervening.c, first malloc");
		exit(-1);
	}

	current->total_arrays = 0;
	current->rowId = NULL;

	return current;
}


// intervening_struct *updateInterveningStruct(intervening *intervening_struct, uint64_t *array_A, uint64_t *array_B){

// }

void pushJoinedElements_v2(uint64_t payload_A, uint64_t payload_B, intervening_array *array_A, intervening_array *array_B){
	// An oxi monadiko payload, push, alliws continue.
	if (array_A->payload_array == NULL)
	{
		array_A->payload_array = ((intervening_array*)malloc(((array_A->position)+1)*sizeof(uint64_t)));
		array_A->payload_array[position] = payload_A;
		array_A->position++;
	}
	else
	{
		if (array_A->payload_array[position-1] != payload_A)
		{
			/* code */
		}
		array_A->payload_array = ((intervening_array*)realloc(array_A->payload_array, (position+1)*sizeof(uint64_t)));
		array_A->payload_array[position] = payload_A;
		array_A->position++;
	}
}


void Join_v2(intervening_struct *intervening_struct, relation *rel_A, relation *rel_B, intervening_array *array_A, intervening_array *array_B){
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
				pushJoinedElements_v2(rel_A->tuples[a].payload, rel_B->tuples[b].payload, array_A, array_B);
				if(b==rel_B->num_tuples){
					return counter;
				}
				b++;
				counter++;
			}
			a++;
			if(a==rel_A->num_tuples){
				return counter;
			}
		}

	}

}