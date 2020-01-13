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

#define THREADS 2

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
    if(argc == 2)
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

    uint64_t totalQueries = wl_ptr -> num_parameters;




    // scheduler->jobs_left = scheduler->total_threads;
    #if THREADS > 1
        scheduler->jobs_left = totalQueries;
    #endif

    begin = clock();
    for (uint64_t i = 0; i < totalQueries; i++)
    {

        #if THREADS == 1
            Execute_Queries(md, wl_ptr, i, stats , c);
        #endif

        #if THREADS > 1


            job_query * job_arguments = malloc(sizeof(job_query));
            job_arguments->md = md;
            job_arguments->wl_ptr = wl_ptr;
            job_arguments->stats = stats;
            job_arguments->query = i;
            job_arguments->c = c;
            // printf("edw\n");

            Assign_Job(scheduler, &JobQuery, (void*)job_arguments);
        #endif



    }

    #if THREADS > 1
        Barrier(scheduler);
    #endif

    // block until all threads finish their jobs
    // #if THREADS > 1
    //     Barrier(scheduler);
    // #endif
    // relation * rel = Create_Relation(md,0,0);
    //
    // histogram struct_h;
    // histogram * hist = &struct_h;
    //
    // Histogram(rel,hist,7,0,rel -> num_tuples);
    // Print_Histogram(hist);
    //
    // exit(-1);

    // #if THREADS > 1
    //     relation * rel = Create_Relation(md,0,0);
    //     scheduler->jobs_left = scheduler->total_threads;
    //     histogram ** hist = calloc(scheduler -> total_threads, sizeof(histogram *));
    //     uint64_t tuples_per_thread = (rel -> num_tuples) / scheduler -> total_threads;
    //     uint64_t tuples_per_thread_final =  (rel -> num_tuples) / scheduler -> total_threads;
    //
    //     for (size_t j = 0; j < scheduler -> total_threads; j++)
    //     {
    //         job_hist * job_arguments = malloc(sizeof(job_hist));
    //         job_arguments -> rel = rel;
    //         job_arguments -> hist = &hist[j];
    //         job_arguments -> sel_byte = 7;
    //         job_arguments -> start = j * tuples_per_thread;
    //         if(j + 1 == scheduler -> total_threads)
    //         {
    //             job_arguments -> end = rel -> num_tuples;
    //         }
    //         else
    //         {
    //            job_arguments -> end = (j + 1) * tuples_per_thread;
    //         }
    //         // Assign_Job(scheduler, &JobHist, (void*)job_arguments);
    //         // Print_Histogram(job_arguments -> hist);
    //     }
    //
    //     uint64_t * hist_final = calloc(256, sizeof(uint64_t));
    //     #if THREADS > 1
    //     // block until all threads finish their jobs
    //         // Barrier(scheduler);
    //     #endif
    //
    //     for (size_t i = 0; i < 256; i++)
    //     {
    //         for (size_t j = 0; j < scheduler -> total_threads; j++)
    //         {
    //             // hist_final[i] += hist[j][i];
    //         }
    //     }
    //     // Print_Histogram(hist_final);
    //     // job_arguments -> rel = md;
    //     // job_arguments -> wl_ptr = wl_ptr;
    //     // job_arguments -> stats = stats;
    //     // job_arguments -> query = i;
    //     // job_arguments -> c = c;
    //
    //     // Assign_Job(scheduler, &JobHist, (void*)job_arguments);
    // #endif



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

    printf("Time %f\n", time_spent);


    return 0;
}
