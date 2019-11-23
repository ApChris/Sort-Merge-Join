#include <stdio.h>
#include <time.h>
#include <string.h>
#include "../include/getColumn.h"
#include "../include/initArray.h"
#include "../include/reorderedColumn.h"
#include "../include/quicksort.h"
#include "../include/splitBucket.h"
#include "../include/result.h"
#include "../include/sortMergeJoin.h"
#include "../include/processRelation.h"


int main(int argc, char const *argv[])
{

    metadata * md;

    uint64_t * ptr;

    md = Read_Init_Binary("workloads/small/small.init");

    for (size_t i = 0; i < 14; i++)
    {
        printf("%lu) tuples = %lu\tcolumns = %lu\t----> ",i,  md[i].num_tuples, md[i].num_columns);

        for (size_t j = 0; j < md[i].num_columns; j++)
        {
            ptr = md[i].array[j];
            printf("%lu|",*(ptr));
        }
        printf("\n");
    }

    printf("\n\n\n");

    Read_Work("workloads/small/small.work");

    relation struct_A;
    relation * relation_A = &struct_A;

    relation struct_A_final;
    relation * relation_A_final = &struct_A_final;
    Create_Relation(md,3,3,relation_A);
    Final_Relation(relation_A, relation_A_final);

    for (size_t i = 0; i < relation_A_final -> num_tuples; i++)
    {
        printf("%lu)\t%lu\n", relation_A_final->tuples[i].payload, relation_A_final->tuples[i].key );
    }

    relation struct_B;
    relation * relation_B = &struct_B;

    relation struct_B_final;
    relation * relation_B_final = &struct_B_final;
    Create_Relation(md,0,0,relation_B);
    Final_Relation(relation_B, relation_B_final);


    relation struct_B_filtered;
    relation * relation_B_filtered = &struct_B_filtered;

    Filter(relation_B_final,3000,'>',relation_B_filtered);


    for (size_t i = 0; i < relation_B_filtered -> num_tuples; i++)
    {
        printf("%lu)\t%lu\n", relation_B_filtered->tuples[i].payload, relation_B_filtered->tuples[i].key );
    }


    return 0;
}
