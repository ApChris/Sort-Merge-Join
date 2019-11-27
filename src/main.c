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

    relation struct_A_final;
    relation * relation_A_final = &struct_A_final;
    Radix_Sort(relation_A, relation_A_final);



    relation struct_B;
    relation * relation_B = &struct_B;

    // r2.0
    Create_Relation(md,2,0,relation_B);

    relation struct_B_final;
    relation * relation_B_final = &struct_B_final;
    Radix_Sort(relation_B, relation_B_final);

    // r1.1 = r2.0

    intervening *interv_final = interveningInit();

    Join_v2(interv_final, relation_A_final, relation_B_final, 1, 0);
    Print_Relation_2(interv_final->final_rel);



    


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

    //Print_Relation_2(relation_A);

    // relation struct_A_final;
    // relation * relation_A_final = &struct_A_final;
    // Radix_Sort(relation_A, relation_A_final);
    // //
    //  Print_Relation_2(relation_A_final);

    // printf("%lu\n", relation_A_final -> num_tuples);

//
//     relation struct_A;
//     relation * relation_A = &struct_A;
//
//     relation struct_A_final;
//     relation * relation_A_final = &struct_A_final;
//     Create_Relation(md,5,1,relation_A);
//     Final_Relation(relation_A, relation_A_final);
//
//     for (size_t i = 0; i < relation_A_final -> num_tuples; i++)
//     {
//         printf("%lu)\t%lu\n", relation_A_final->tuples[i].payload, relation_A_final->tuples[i].key );
//     }
//
//     relation struct_B;
//     relation * relation_B = &struct_B;
//
//     relation struct_B_final;
//     relation * relation_B_final = &struct_B_final;
//     Create_Relation(md,0,0,relation_B);
//     Final_Relation(relation_B, relation_B_final);
//
//
//     relation struct_B_filtered;
//     relation * relation_B_filtered = &struct_B_filtered;
//
//     Filter(relation_B_final,3000,'<',relation_B_filtered);
//
//
//     for (size_t i = 0; i < relation_B_filtered -> num_tuples; i++)
//     {
//         printf("%lu)\t%lu\n", relation_B_filtered->tuples[i].payload, relation_B_filtered->tuples[i].key );
//     }
//
//     intervening *interveningA = intervening_Init();
//      intervening_array *array_A = intervening_Array_Init();
//       intervening_array *array_B = intervening_Array_Init();
//     uint64_t counter = Join_v2(interveningA,relation_A_final,relation_B_filtered,array_A,array_B);
//     printf("counter = %lu\n",counter);
//
// for (size_t i = 0; i < array_A -> position; i++)
// {
//     /* code */
//     printf("%lu\n",array_A -> payload_array[i]);
// }
//
// uint64_t sum = 0;
//
// sum = Sum_Column(array_A);
//
// printf("Sum = %lu\n",sum);








// 5 1|0.1=1.0&0.2=4531|1.2
// result 9283


//     relation struct_A;
//     relation * relation_A = &struct_A;
//
//     relation struct_A_final;
//     relation * relation_A_final = &struct_A_final;
//     Create_Relation(md,5,1,relation_A);
//     Final_Relation(relation_A, relation_A_final);
//
//     relation struct_A_2;
//     relation * relation_A_2 = &struct_A_2;
//
//     relation struct_A_final_2;
//     relation * relation_A_final_2 = &struct_A_final_2;
//     Create_Relation(md,5,2,relation_A_2);
//     Final_Relation(relation_A_2, relation_A_final_2);
//
//     // for (size_t i = 0; i < relation_A_final -> num_tuples; i++)
//     // {
//     //     printf("%lu)\t%lu\n", relation_A_final->tuples[i].payload, relation_A_final->tuples[i].key );
//     // }
//
//     relation struct_B;
//     relation * relation_B = &struct_B;
//
//     relation struct_B_final;
//     relation * relation_B_final = &struct_B_final;
//     Create_Relation(md,1,0,relation_B);
//     Final_Relation(relation_B, relation_B_final);
//
//
//     relation struct_A_filtered;
//     relation * relation_A_filtered = &struct_A_filtered;
//
//     Filter(relation_A_final_2,4531,'=',relation_A_filtered);
//
//
//     for (size_t i = 0; i < relation_A_filtered -> num_tuples; i++)
//     {
//         printf("%lu)\t%lu\n", relation_A_filtered->tuples[i].payload, relation_A_filtered->tuples[i].key );
//     }
//
//     intervening *interveningA = intervening_Init();
//     intervening_array *array_A = intervening_Array_Init();
//     intervening_array *array_B = intervening_Array_Init();
// //    uint64_t counter = Join_v2(interveningA,relation_A_final,relation_B_final,array_A,array_B);
// //  printf("counter = %lu\n",counter);
//
// for (size_t i = 0; i < array_A -> position; i++)
// {
//     /* code */
//     printf("%lu\n",array_A -> payload_array[i]);
// }
//
// uint64_t sum = 0;
//
// sum = Sum_Column(array_A);
//
// printf("Sum = %lu\n",sum);
//
// uint64_t *a;
// a = (uint64_t *)malloc(sizeof(uint64_t)* 5);
// for (size_t i = 0; i < 5; i++)
// {
//     a[i] = rand();
// }
// for (size_t i = 0; i < 5; i++)
// {
//     printf("%lu\n", a[i]);
// }
//
// Quicksort_v2(a,0,5-1);
//
// printf("\n" );
//
// for (size_t i = 0; i < 5; i++)
// {
//     printf("%lu\n", a[i]);
// }

    return 0;
}
