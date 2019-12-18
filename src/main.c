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
#include "../include/work.h"
#include "../include/executeQuery.h"


int main(int argc, char const *argv[])
{

    metadata * md;
    work_line * wl_ptr;

    uint64_t num_rows;
    double time_spent = 0;
    double begin , end;
    if(argc == 2)
    {
        if(!strcmp(argv[1],"small"))
        {
            md = Read_Init_Binary("workloads/small/small.init","workloads/small/",&num_rows);
            wl_ptr = Read_Work("workloads/small/small.work");
        }
        else if(!strcmp(argv[1],"medium"))
        {
            md = Read_Init_Binary("workloads/medium/medium.init","workloads/medium/",&num_rows);
            // wl_ptr = Read_Work("workloads/medium/medium.work");
        }
        else
        {
            printf("Wrong argument! Try again\n");
            exit(-1);
        }
    }
    else if(argc > 2)
    {
        printf("\nToo many arguments! Try again\n\n");
        exit(-1);
    }
    else
    {
        printf("\nYou have to run one of the following:\n./smj small\n./smj medium\n\nTry again!!\n\n");
        exit(-1);
    }

    // Print_Work(wl_ptr);
    uint64_t totalQueries = wl_ptr -> num_parameters;

    for (uint64_t i = 0; i < 1; i++)
    {
        begin = clock();

        // Execute_Queries(md, wl_ptr, i);
        end = clock();

        time_spent += (double)(end - begin) / CLOCKS_PER_SEC;
    }
    printf("Edw\n");
    uint64_t * ptr;
    ptr = md[0].array[0];
    printf("%lu %lu\n",*(ptr+ 0),md[0].num_tuples);
    printf("Edw\n");
    relation * rel = Create_Relation(md,0,1);
    Print_Relation_2(rel);
    printf("num_tuples : %lu\n",rel -> num_tuples);
    rel = Radix_Sort(rel);
    // Print_Relation_2(rel);
    rel = Filter(rel,10000,'>');

    relation * rel2 = Create_Relation(md,0,1);
    Print_Relation_2(rel2);
    printf("num_tuples : %lu\n",rel2 -> num_tuples);
    rel2 = Radix_Sort(rel2);
    // Print_Relation_2(rel);
    rel2 = Filter(rel2,10000,'>');
    printf("num_tuples : %lu\n",rel2 -> num_tuples);

    intervening * interv_final = interveningInit();
    Join_v2(interv_final, rel,rel2,0,1);

    for (size_t i = 0; i < interv_final -> final_rel -> num_tuples; i++)
    {
        free(interv_final -> final_rel ->tuples[i].payload);
    }
    free(interv_final -> final_rel ->tuples);
    free(interv_final -> final_rel);
    //
    free(interv_final -> rowId);
    free(interv_final);


    for(size_t i = 0; i < rel -> num_tuples; i++)
    {
        free(rel->tuples[i].payload);
    }
    free(rel->tuples);
    free(rel);

    for(size_t i = 0; i < rel2 -> num_tuples; i++)
    {
        free(rel2->tuples[i].payload);
    }
    free(rel2->tuples);
    free(rel2);
    // printf("%lu",num_rows);

    for (uint64_t i = 0; i < num_rows; i++)
    {
        free(md[i].array);
        free(md[i].full_array);
    }
    free(md);



    for (size_t j = 0; j < wl_ptr -> num_parameters; j++)
    {
        free(wl_ptr -> parameters[j].tuples);//.tuples[k]);

    }
    for (size_t j = 0; j < wl_ptr -> num_predicates; j++)
    {
        free(wl_ptr -> predicates[j].tuples);//.tuples[k]);
    }
    for (size_t j = 0; j < wl_ptr -> num_selects; j++)
    {
        free(wl_ptr -> selects[j].tuples);//.tuples[k]);
    }
    for (size_t j = 0; j < wl_ptr -> num_filters; j++)
    {
        free(wl_ptr -> filters[j].tuples);//.tuples[k]);
    }



    free(wl_ptr -> parameters);
    free(wl_ptr -> predicates);
    free(wl_ptr -> selects);
    free(wl_ptr -> filters);
    free(wl_ptr);
    // printf("Time %f\n", time_spent);
    return 0;
}


// correct medium results
// i = 1, 4, 12, 18, 20, 22, 27, 31, 32, 33, 37, 40, 42, 46
