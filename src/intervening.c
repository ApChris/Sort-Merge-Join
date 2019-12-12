#include "../include/intervening.h"
#include "../include/relation.h"

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

void pushJoinedElements_v2(relation * temp_rel, relation * relation_A, relation * relation_B, uint64_t posA, uint64_t posB, uint64_t counter, uint64_t key)
{
	uint64_t number_of_paylods = 0;
	if (counter == 0)
	{
		temp_rel -> tuples = (tuple*)malloc(sizeof(tuple));
		temp_rel -> tuples[counter].position = relation_A -> tuples[posA].position + relation_B -> tuples[posB].position;
		temp_rel -> tuples[counter].payload = (uint64_t*)malloc(sizeof(uint64_t)*temp_rel -> tuples[counter].position);

		for (size_t i = 0; i < relation_A -> tuples[posA].position; i++)
		{
			temp_rel->tuples[counter].payload[number_of_paylods] = relation_A -> tuples[posA].payload[i];
			number_of_paylods++;
		}
		for (size_t i = 0; i < relation_B -> tuples[posB].position; i++)
		{
			temp_rel -> tuples[counter].payload[number_of_paylods] = relation_B -> tuples[posB].payload[i];
			number_of_paylods++;
		}

		temp_rel -> tuples[counter].key = key;
		temp_rel -> num_tuples = 1;

	}
	else
	{
		temp_rel -> tuples = (tuple*)realloc(temp_rel->tuples, (sizeof(tuple)*(counter+1)));
	 	temp_rel -> tuples[counter].position = relation_A -> tuples[posA].position + relation_B -> tuples[posB].position;
		temp_rel -> tuples[counter].payload = (uint64_t*)malloc(sizeof(uint64_t)*temp_rel -> tuples[counter].position);
		for (size_t i = 0; i < relation_A -> tuples[posA].position; i++)
		{
			temp_rel->tuples[counter].payload[number_of_paylods] = relation_A -> tuples[posA].payload[i];
			number_of_paylods++;
		}
		for (size_t i = 0; i < relation_B -> tuples[posB].position; i++)
		{
			temp_rel -> tuples[counter].payload[number_of_paylods] = relation_B -> tuples[posB].payload[i];
			number_of_paylods++;
		}
		temp_rel -> tuples[counter].key = key;
		temp_rel -> num_tuples++;
	}
}


void pushJoinedElements_Self(relation * temp_rel, relation * relation_A, uint64_t posA, uint64_t counter, uint64_t key)
{
	uint64_t number_of_paylods = 0;
	if (counter == 0)
	{
		temp_rel -> tuples = (tuple*)malloc(sizeof(tuple));
		temp_rel -> tuples[counter].position = relation_A -> tuples[posA].position;
		temp_rel -> tuples[counter].payload = (uint64_t*)malloc(sizeof(uint64_t)*temp_rel -> tuples[counter].position);

		for (size_t i = 0; i < relation_A -> tuples[posA].position; i++)
		{
			temp_rel->tuples[counter].payload[number_of_paylods] = relation_A -> tuples[posA].payload[i];
			number_of_paylods++;
		}
		temp_rel -> tuples[counter].key = key;
		temp_rel -> num_tuples = 1;

	}
	else
	{
		temp_rel -> tuples = (tuple*)realloc(temp_rel->tuples, (sizeof(tuple)*(counter+1)));
	 	temp_rel -> tuples[counter].position = relation_A -> tuples[posA].position;
		temp_rel -> tuples[counter].payload = (uint64_t*)malloc(sizeof(uint64_t)*temp_rel -> tuples[counter].position);
		for (size_t i = 0; i < relation_A -> tuples[posA].position; i++)
		{
			temp_rel->tuples[counter].payload[number_of_paylods] = relation_A -> tuples[posA].payload[i];
			number_of_paylods++;
		}
		temp_rel -> tuples[counter].key = key;
		temp_rel -> num_tuples++;
	}
}


uint64_t Join_v2(intervening * final_interv, relation * rel_A, relation * rel_B, uint64_t rowIdA, uint64_t rowIdB)
{
	// final_interv has intervening results before joining relA and relB.
	// In the end, final_interv will also have the result of the relA=relB

	relation struct_A;
	relation * temp_rel = &struct_A;

	uint64_t join_flag = 0;
	uint64_t flag = 0;
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
				// 1st iteration
				if (join_flag == 0)
				{
					// 1st join
					if (final_interv -> position == NULL)
					{
						// I have to add self join
						final_interv -> rowId = (uint64_t*)malloc(sizeof(uint64_t)*2);
						final_interv -> rowId[0] = rowIdA;
						final_interv -> rowId[1] = rowIdB;
						final_interv -> position = 2;

						temp_rel = (relation *)malloc(sizeof(relation));
						relation struct_final;
						final_interv -> final_rel = &struct_final;
						final_interv -> final_rel -> num_tuples = 0;
						printf("join malloc\n");

					}
					// rest joins
					else
					{

						if(FindRowID(final_interv,rowIdA) != TAG) // if exists
						{
							if(FindRowID(final_interv,rowIdB) != TAG) // if exists
							{
								printf("Yparxoun!!!\n");
								temp_rel = (relation *)malloc(sizeof(relation));
								relation struct_final;
								final_interv -> final_rel = &struct_final;
								final_interv -> final_rel -> num_tuples = 0;
								flag = 1;
							}
							else
							{
								final_interv -> rowId = (uint64_t*)realloc(final_interv->rowId, (sizeof(uint64_t)*((final_interv -> position) + 1)));
								final_interv -> rowId[final_interv -> position] = rowIdB;

								final_interv -> position += 1;
								temp_rel = (relation *)malloc(sizeof(relation));
								relation struct_final;
								final_interv -> final_rel = &struct_final;
								final_interv -> final_rel -> num_tuples = 0;
							}

						}
						else // if you find it
						{
							final_interv -> rowId = (uint64_t*)realloc(final_interv->rowId, (sizeof(uint64_t)*((final_interv -> position) + 1)));
							final_interv -> rowId[final_interv -> position] = rowIdA;

							final_interv -> position += 1;
							temp_rel = (relation *)malloc(sizeof(relation));
							relation struct_final;
							final_interv -> final_rel = &struct_final;
							final_interv -> final_rel -> num_tuples = 0;
						}



					//	printf("join realloc\n");
					}
					join_flag = 1;

				}

				// // push
				// if(flag)
				// {
				// 	pushJoinedElements_Self(temp_rel, rel_A, a, counter,rel_A->tuples[a].key);
				// }
				// else
				// {
					pushJoinedElements_v2(temp_rel, rel_A, rel_B, a, b, counter,rel_A->tuples[a].key);

				// }

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
	final_interv -> final_rel = temp_rel;
	return counter;
}

uint64_t FindRowID(intervening * final_interv, uint64_t rowID)
{
//	printf("pos = %lu\n", final_interv -> position);
	for (size_t i = 0; i < final_interv -> position; i++)
	{
		if(final_interv -> rowId[i] == rowID)
		{
			return i;
		}
	}
	return TAG;
}


uint64_t Self_Join(intervening * final_interv, relation * rel_A, relation * rel_B, uint64_t rowIdA, uint64_t rowIdB)
{

	relation struct_A;
	relation * temp_rel = &struct_A;

	int join_flag = 0;

	uint64_t counter = 0;

	uint64_t mark = 0, a = 0, b = 0;
	uint64_t flag = 0;
	printf("tuples %lu\n",rel_A -> num_tuples);
	for (size_t i = 0; i < rel_A -> num_tuples; i++)
	{
	//	printf("%lu) %lu ---- %lu \n",i, rel_A->tuples[i].key, rel_B->tuples[i].key);
		if(rel_A->tuples[i].key == rel_B->tuples[i].key)
		{
			if (join_flag == 0)
			{
				// 1st join
				if (final_interv -> position == NULL)
				{
					// I have to add self join
					final_interv -> rowId = (uint64_t*)malloc(sizeof(uint64_t)*2);
					final_interv -> rowId[0] = rowIdA;
					final_interv -> rowId[1] = rowIdB;
					final_interv -> position = 2;

					temp_rel = (relation *)malloc(sizeof(relation));
					relation struct_final;
					final_interv -> final_rel = &struct_final;
					final_interv -> final_rel -> num_tuples = 0;
					printf("join malloc\n");

				}
				// rest joins
				else
				{

					if(FindRowID(final_interv,rowIdA) != TAG) // if exists
					{
						if(FindRowID(final_interv,rowIdB) != TAG) // if exists
						{
								printf("Yparxoun!!!\n");
							temp_rel = (relation *)malloc(sizeof(relation));
							relation struct_final;
							final_interv -> final_rel = &struct_final;
							final_interv -> final_rel -> num_tuples = 0;
							flag = 1;
						}
						else
						{
							final_interv -> rowId = (uint64_t*)realloc(final_interv->rowId, (sizeof(uint64_t)*((final_interv -> position) + 1)));
							final_interv -> rowId[final_interv -> position] = rowIdB;

							final_interv -> position += 1;
							temp_rel = (relation *)malloc(sizeof(relation));
							relation struct_final;
							final_interv -> final_rel = &struct_final;
							final_interv -> final_rel -> num_tuples = 0;
						}
					}
					else // if you find it
					{
						final_interv -> rowId = (uint64_t*)realloc(final_interv->rowId, (sizeof(uint64_t)*((final_interv -> position) + 1)));
						final_interv -> rowId[final_interv -> position] = rowIdA;

						final_interv -> position += 1;
						temp_rel = (relation *)malloc(sizeof(relation));
						relation struct_final;
						final_interv -> final_rel = &struct_final;
						final_interv -> final_rel -> num_tuples = 0;
					}

				}
				join_flag = 1;

			}

			if(flag)
			{
				printf("Mpainei!!\n");
				pushJoinedElements_Self(temp_rel, rel_A, i, counter,rel_A->tuples[a].key);
			}
			else
			{
				pushJoinedElements_v2(temp_rel, rel_A, rel_B, i, i, counter,rel_A->tuples[a].key);
			}
			counter++;
		}

	}
	final_interv -> final_rel = temp_rel;
	return counter;
}
//
// relation * Self_Join_2(relation * rel_A, relation * rel_Bs)
// {
//
// 	relation struct_A;
// 	relation * temp_rel = &struct_A;
//
// 	int join_flag = 0;
//
// 	uint64_t counter = 0;
//
// 	uint64_t mark = 0, a = 0, b = 0;
// 	uint64_t flag = 0;
// 	printf("tuples %lu\n",rel_A -> num_tuples);
//
// 	temp_rel = (relation *)malloc(sizeof(relation));
// 	temp_rel -> num_tuples = 0;
// 	while(a < rel_A->num_tuples && b < rel_B->num_tuples)
// 	{
//
// 			while(rel_A->tuples[a].key < rel_B->tuples[b].key)
// 			{
// 				a++;
// 				if(a==rel_A->num_tuples){
// 					final_interv -> final_rel = temp_rel;
// 					return counter;
// 				}
//
// 			}
// 			while(rel_A->tuples[a].key > rel_B->tuples[b].key)
// 			{
// 				b++;
// 				if(b==rel_B->num_tuples){
// 					final_interv -> final_rel = temp_rel;
// 					return counter;
// 				}
//
// 			}
// 			mark = b;
//
//
// 		while (rel_A->tuples[a].key == rel_B->tuples[mark].key)
// 		{
// 			b = mark;
// 			while (rel_A->tuples[a].key == rel_B->tuples[b].key)
// 			{
//
// 				if(flag)
// 				{
// 					pushJoinedElements_Self(temp_rel, rel_A, a, counter,rel_A->tuples[a].key);
// 				}
// 				else
// 				{
// 					pushJoinedElements_v2(temp_rel, rel_A, rel_B, a, b, counter,rel_A->tuples[a].key);
// 				}
//
// 				if(b==rel_B->num_tuples)
// 				{
// 					final_interv -> final_rel = temp_rel;
// 					return counter;
// 				}
// 				b++;
// 				counter++;
// 			}
// 			a++;
// 			if(a==rel_A->num_tuples)
// 			{
// 				final_interv -> final_rel = temp_rel;
// 				return counter;
// 			}
// 		}
//
// 	}
// 	final_interv -> final_rel = temp_rel;
// 	return counter;
// }


relation * Scan(relation * rel_A, relation * rel_B)
{



	int join_flag = 0;

	uint64_t counter = 0;

	uint64_t mark = 0, a = 0, b = 0;
	uint64_t flag = 0;

	relation * temp_rel = (relation *)malloc(sizeof(relation));
	temp_rel -> num_tuples = 0;
	for (size_t i = 0; i < rel_A -> num_tuples; i++)
	{
		if(rel_A->tuples[i].key == rel_B->tuples[i].key)
		{
			pushJoinedElements_Self(temp_rel, rel_A, i, counter,rel_A->tuples[i].key);

			counter++;
		}

	}
	return temp_rel;
}
