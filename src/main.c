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
#include "../include/intervening.h"


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

    //Read_Work("workloads/small/small.work");

    relation struct_A;
    relation * relation_A = &struct_A;

    // r1.1
    Create_Relation(md,1,1,relation_A);

    // Update_Tuple_Payload(md,relation_A,1560,99999999,9999);
    // Update_Tuple_Payload(md,relation_A,1560,99999999,789);
    // Update_Tuple_Payload(md,relation_A,1560,99999999,32);
    // Update_Tuple_Payload(md,relation_A,1560,99999999,450);
    //
    // Update_Tuple_Payload(md,relation_A,1560,99999999,72);
    // Update_Tuple_Payload(md,relation_A,1560,99999999,65);
    //
    // Update_Tuple_Payload(md,relation_A,1559,8888888,72);
    // Update_Tuple_Payload(md,relation_A,1559,8888888,65);
    // Update_Tuple_Payload(md,relation_A,1559,8888888,72);
    // Update_Tuple_Payload(md,relation_A,1558,7777777,78);



    relation struct_A_final;
    relation * relation_A_final = &struct_A_final;
    Radix_Sort(relation_A, relation_A_final);



    relation struct_B;
    relation * relation_B = &struct_B;

    // r2.0
    Create_Relation(md,2,0,relation_B);

    // Update_Tuple_Payload(md,relation_B,66,8888888,99);
    // Update_Tuple_Payload(md,relation_B,20,7777777,23);

    relation struct_B_final;
    relation * relation_B_final = &struct_B_final;
    Radix_Sort(relation_B, relation_B_final);

    // r1.1 = r2.0

    intervening * interv_final = interveningInit();

    Join_v2(interv_final, relation_A_final, relation_B_final, 2, 3);
    Print_Relation_2(interv_final -> final_rel);

    uint64_t counter = Join_v2(interv_final, interv_final->final_rel, relation_B_final, 2, 3);

    relation struct_A_B;
    relation * rel_final = &struct_A_B;
    Print_Relation_2(interv_final->final_rel);
    interv_final->final_rel = Filter(interv_final->final_rel, 5710, '>');

    //Print_Relation_2(interv_final -> final_rel);
    Print_Relation_2(interv_final->final_rel);

    printf("results = %lu\n",counter);
    // if(FindRowID(interv_final,2))
    // {
    //     printf("Found\n");
    // }
    // else
    // {
    //     printf("Not found\n");
    // }
    // Update_Tuple_Payload(md,relation_A,1560,99999999,9999);
    // Update_Tuple_Payload(md,relation_A,1560,99999999,789);
    // Update_Tuple_Payload(md,relation_A,1560,99999999,32);
    // Update_Tuple_Payload(md,relation_A,1560,99999999,450);

    // Update_Tuple_Payload(md,relation_A,1560,99999999,72);
    // Update_Tuple_Payload(md,relation_A,1560,99999999,65);

    // Update_Tuple_Payload(md,relation_A,1559,8888888,72);
    // Update_Tuple_Payload(md,relation_A,1559,8888888,65);
    // Update_Tuple_Payload(md,relation_A,1559,8888888,72);
    // Update_Tuple_Payload(md,relation_A,1558,7777777,65);

    // Print_Relation_2(relation_A);

    // relation struct_A_final;
    // relation * relation_A_final = &struct_A_final;
    // Radix_Sort(relation_A, relation_A_final);
    // //
    //  Print_Relation_2(relation_A_final);

    // printf("%lu\n", relation_A_final -> num_tuples);





    return 0;
}
