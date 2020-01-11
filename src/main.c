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

#define THREADS 4

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
    if(argc == 2)
    {
        if(!strcmp(argv[1],"small"))
        {
            md = Read_Init_Binary("workloads/small/small.init","workloads/small/",&num_rows, stats);
            wl_ptr = Read_Work("workloads/small/small.work");
            stats = Calculate_Statistics(md, num_rows);

        }
        else if(!strcmp(argv[1],"medium"))
        {
            md = Read_Init_Binary("workloads/medium/medium.init","workloads/medium/",&num_rows, stats);
            wl_ptr = Read_Work("workloads/medium/medium.work");
           stats = Calculate_Statistics(md, num_rows);
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

    // Correct: 0, 1, 3, 4, 5, 6, 8, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26
    //          27, 28, 29, 30, 31, 32, 33, 34 , 36, 37, 39, 40, 41, 42, 44, 45, 46, 47, 48
    // Killed: 2, 7, 11
    // Cannot allocate memory : 35 (Maybe that's random)
    // 38, 43 SEG: Case that Join doesn't Find any result, so it has to terminate



    // #if NUM_THREADS > 1
    //     Scheduler_Init(&sched,NUM_THREADS);
    // #endif

    // scheduler->jobs_left = scheduler->total_threads;
    scheduler->jobs_left = totalQueries;

    for (uint64_t i = 0; i < totalQueries; i++)
    {
        // if(i == 2)// || i == 7 || i == 11 || i == 35)
        // {
        //     printf("697086818074004 30421427867517 30424207300885\n");
        //     continue;
        // }
        // if(i == 7)
        // {
        //     printf("263815608355 419220319059540 19761887342801\n");
        //     continue;
        // }
        // if(i == 11)
        // {
        //     printf("60031231103105 60030577889893\n");
        //     continue;
        // }
        // else if(i == 35)
        // {
        //     printf("NULL NULL NULL\n");
        //     continue;
        // }
        // else if(i == 42)
        // {
        //     printf("NULL NULL NULL\n");
        //     continue;
        // }
        // else if(i == 43)
        // {
        //     printf("NULL NULL NULL\n");
        //     continue;
        // }

        begin = clock();

        job_query * job_arguments = malloc(sizeof(job_query));
        job_arguments->md = md;
        job_arguments->wl_ptr = wl_ptr;
        job_arguments->stats = stats;
        job_arguments->query = i;
        job_arguments->c = 's';
        // for medium
        // job_arguments->c = 'm';

        Assign_Job(scheduler, &JobQuery, (void*)job_arguments);

        // Execute_Queries(job_arguments->md, job_arguments->wl_ptr, job_arguments->query, job_arguments->stats);

        // Execute_Queries(md, wl_ptr, i, stats);
        // printf("enma\n");
        end = clock();
        time_spent += (double)(end - begin) / CLOCKS_PER_SEC;
    }
    
    // block until all threads finish their jobs
    Barrier(scheduler);

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



// Write_To_File(md, "output/r8.txt",8);


void Write_To_File(metadata * md, char * filename,uint64_t pos)
{
    uint64_t * ptr;
    FILE *fp;
    if((fp = fopen(filename,"w")) == NULL)
    {
        perror("fopen failed");
        exit(-1);
    }

        for (size_t j = 0; j < md[pos].num_tuples; j++)
        {
            for (size_t z = 0; z <  md[pos].num_columns; z++)
            {
                /* code */
                ptr = md[pos].array[z];

                char str[30];

                sprintf(str,"%lu",*(ptr + j));
                fputs(str,fp);
                fputs("|",fp);


            }
            fputs("\n",fp);
            // printf("%lu|", *(ptr));
        }


    fclose(fp);
}

// correct medium results
// i = 1, 4, 12, 18, 20, 22, 27, 31, 32, 33, 37, 40, 42, 46
