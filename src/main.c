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

    double time_spent = 0;
    double begin , end;
    if(argc == 2)
    {
        if(!strcmp(argv[1],"small"))
        {
            md = Read_Init_Binary("workloads/small/small.init","workloads/small/");
            wl_ptr = Read_Work("workloads/small/small.work");
        }
        else if(!strcmp(argv[1],"medium"))
        {
            md = Read_Init_Binary("workloads/medium/medium.init","workloads/medium/");
            wl_ptr = Read_Work("workloads/medium/medium.work");
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


    for (uint64_t i = 0; i < totalQueries; i++)
    {
        begin = clock();

        Execute_Queries(md, wl_ptr, i);
        end = clock();

        time_spent += (double)(end - begin) / CLOCKS_PER_SEC;
    }

    // printf("Time %f\n", time_spent);
    return 0;
}
