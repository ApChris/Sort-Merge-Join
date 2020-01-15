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
#include "../include/bestTree.h"
#include "../include/jobScheduler.h"


int main(int argc, char const *argv[])
{
    job_scheduler * scheduler;
    #if THREADS > 1
        scheduler = Init_JobScheduler(THREADS);
    #endif

    metadata * md;
    work_line * wl_ptr;
    statistics * stats;
    uint64_t num_rows;
    double time_spent = 0;
    double begin , end;
    char c;
    if(argc == 3)
    {
        if(!strcmp(argv[1],"small"))
        {
            md = Read_Init_Binary("workloads/small/small.init","workloads/small/",&num_rows, stats);
            wl_ptr = Read_Work("workloads/small/small.work");
            stats = Calculate_Statistics(md, num_rows);
            c = 's';
        }
        else if(!strcmp(argv[1],"medium"))
        {
            md = Read_Init_Binary("workloads/medium/medium.init","workloads/medium/",&num_rows, stats);
            wl_ptr = Read_Work("workloads/medium/medium.work");
            stats = Calculate_Statistics(md, num_rows);
            c = 'm';
        }
        else
        {
            printf("Wrong argument! Try again\n");
            exit(-1);
        }
    }
    else if(argc > 3)
    {
        printf("\nToo many arguments! Try again\n\n");
        exit(-1);
    }
    else
    {
        printf("\nYou have to run one of the following:\n./smj small query/radix\n./smj medium query/radix\n\nTry again!!\n\n");
        exit(-1);
    }

    uint64_t totalQueries = wl_ptr -> num_parameters;


    #if THREADS > 1
        if(!strcmp(argv[2],"query"))
        {
            scheduler->jobs_left = 4;
        }
    #endif

    begin = clock();
    for (uint64_t i = 0; i < 4; i++)
    {

        #if THREADS == 1
            Execute_Queries(md, wl_ptr, i, stats , c, scheduler);
        #endif

        #if THREADS > 1
            if(!strcmp(argv[2],"query"))
            {
                job_query * job_arguments = malloc(sizeof(job_query));
                job_arguments -> md = md;
                job_arguments -> wl_ptr = wl_ptr;
                job_arguments -> stats = stats;
                job_arguments -> query = i;
                job_arguments -> c = c;
                job_arguments -> scheduler = scheduler;


                Assign_Job(scheduler, &JobQuery, (void*)job_arguments);
            }
            else if(!strcmp(argv[2],"radix"))
            {
                Execute_Queries(md, wl_ptr, i, stats , c, scheduler);
            }
            else
            {
                printf("\nYou have to run one of the following:\n\n./smj small query/radix\n./smj medium query/radix\n\nTry again!!\n\n");
                exit(-1);
            }

        #endif



    }

    #if THREADS > 1
        Barrier(scheduler);
    #endif


    end = clock();
    time_spent += (double)(end - begin) / CLOCKS_PER_SEC;
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


    //
    for (uint64_t i = 0; i < num_rows; i++)
    {
        free(stats[i].la);
        free(stats[i].ua);
        free(stats[i].fa);
        free(stats[i].size_da);
        free(stats[i].da);
    }
    free(stats);

    #if THREADS > 1
        Destroy_JobScheduler(scheduler);
    #endif

    // printf("Time %f\n", time_spent);


    return 0;
}
