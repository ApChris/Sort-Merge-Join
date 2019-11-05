

#include "../include/sortMergeJoin.h"

result * SortMergeJoin(relation * relation_A_final, relation * relation_B_final)
{
    clock_t begin;
    clock_t end;
    uint64_t counter = 0;
    double time_spent = 0;

    result * node = NULL;
    node = resultInit();

    result * head = node;

    // Join between two sorted relations
    begin = clock();
    counter = Join(relation_A_final, relation_B_final, node);
    end = clock();

    time_spent += (double)(end - begin) / CLOCKS_PER_SEC;

    printf("\n\n------------------------------RESULTS------------------------------\n");
    printf("Elements in Result: %lu\n",counter);
    printf("time = %f s\n",time_spent);
    printf("---------------------------------------------------------------------\n");

    free(relation_A_final -> tuples);
    free(relation_B_final -> tuples);

    return head;
}
