#include <stdio.h>
#include "../include/processRelation.h"



// #define L1_CACHE 65536    //  = 64 kb
#define L1_CACHE 65536

// #define L1_CACHE 32768
void ProcessRelation(relation * rel_old, histogram * hist, psum * ps, relation * rel_new,int64_t sel_byte)
{
    uint64_t i = 0;
    uint64_t size = 0;

    while(i < hist -> num_tuples)
    {
        if(hist -> hist_tuples[i].sum == 0)
        {
            i++;
            continue;
        }

        size = hist -> hist_tuples[i].sum * sizeof(tuple);


        if(size < L1_CACHE)
        {
        //    printf("--------- Bucket : %lu -> %lu elements -> %lu bytes------QUICKSORT------\n",i,hist -> hist_tuples[i].sum,size);
            if(i == hist -> num_tuples - 1)
            {
               Quicksort(rel_new, ps -> psum_tuples[i].position,ps -> psum_tuples[i].position + hist -> hist_tuples[i].sum - 1);
            }
            else
            {

               Quicksort(rel_new,ps -> psum_tuples[i].position,ps -> psum_tuples[i + 1].position -1);

            }
        }
        else        // SPLIT
        {
        //     printf("--------- Bucket : %lu -> %lu elements -> %lu bytes------SPLIT------\n",i,hist -> hist_tuples[i].sum,size);
            histogram struct_h1;
            histogram *hist1 = &struct_h1;
            psum struct_p1;
            psum *ps1 = &struct_p1;
            // BYTE 0
            if(i == hist -> num_tuples - 1)
            {
                Split_Bucket(rel_old,hist1,ps1,rel_new,ps->psum_tuples[i].position, ps -> psum_tuples[i].position + hist -> hist_tuples[i].sum,sel_byte);
            }
            else
            {

                Split_Bucket(rel_old,hist1,ps1,rel_new,ps->psum_tuples[i].position,ps->psum_tuples[i + 1].position,sel_byte);

            }

            // BYTE 1
            for(uint64_t b1 = 0; b1 < hist1 -> num_tuples; b1++)
            {

                if(hist1 -> hist_tuples[b1].sum == 0)
                {
                    continue;
                }
                size = hist1 -> hist_tuples[b1].sum * sizeof(tuple);

                if(size < L1_CACHE)
                {


                //     printf("\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------QUICKSORT------\n",b1,hist1 -> hist_tuples[b1].sum,size);
                    if(b1 == hist1 -> num_tuples - 1)
                    {
                        Quicksort(rel_new, ps1 -> psum_tuples[b1].position,ps1 -> psum_tuples[b1].position + hist1 -> hist_tuples[b1].sum - 1);
                    }
                    else
                    {
                        Quicksort(rel_new,ps1 -> psum_tuples[b1].position,ps1 -> psum_tuples[b1 + 1].position -1);
                    }
                }
                else    // SPLIT
                {
                //     printf("\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------SPLIT------\n",b1,hist1 -> hist_tuples[b1].sum,size);
                    histogram struct_h2;
                    histogram *hist2 = &struct_h2;
                    psum struct_p2;
                    psum *ps2 = &struct_p2;

                    if(b1 == hist1 -> num_tuples - 1)
                    {

                        Split_Bucket(rel_old,hist2,ps2,rel_new,ps1->psum_tuples[b1].position, ps1 -> psum_tuples[b1].position + hist1 -> hist_tuples[b1].sum,5);
                    }
                    else
                    {

                        Split_Bucket(rel_old,hist2,ps2,rel_new,ps1->psum_tuples[b1].position,ps1->psum_tuples[b1 + 1].position,5);
                    }

                    // BYTE 2
                    for(uint64_t b2 = 0; b2 < hist2 -> num_tuples; b2++)
                    {

                        if(hist2 -> hist_tuples[b2].sum == 0)
                        {
                            continue;
                        }
                        size = hist2 -> hist_tuples[b2].sum * sizeof(tuple);

                        if(size < L1_CACHE)
                        {

                        //     printf("\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------QUICKSORT------\n",b2,hist2 -> hist_tuples[b2].sum,size);
                            if(b2 == hist2 -> num_tuples - 1)
                            {
                                Quicksort(rel_new, ps2 -> psum_tuples[b2].position,ps2 -> psum_tuples[b2].position + hist2 -> hist_tuples[b2].sum - 1);
                            }
                            else
                            {
                                Quicksort(rel_new,ps2 -> psum_tuples[b2].position,ps2 -> psum_tuples[b2 + 1].position -1);
                            }
                        }
                        else    // SPLIT
                        {
                        //     printf("\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------SPLIT------\n",b2,hist2 -> hist_tuples[b2].sum,size);
                            histogram struct_h3;
                            histogram *hist3 = &struct_h3;
                            psum struct_p3;
                            psum *ps3 = &struct_p3;
                            if(b2 == hist2 -> num_tuples - 1)
                            {
                                Split_Bucket(rel_old,hist3,ps3,rel_new,ps2->psum_tuples[b2].position, ps2 -> psum_tuples[b2].position + hist2 -> hist_tuples[b2].sum,4);
                            }
                            else
                            {
                                Split_Bucket(rel_old,hist3,ps3,rel_new,ps2->psum_tuples[b2].position,ps2->psum_tuples[b2 + 1].position,4);
                            }

                            // BYTE 3
                            for(uint64_t b3 = 0; b3 < hist3 -> num_tuples; b3++)
                            {
                                if(hist3 -> hist_tuples[b3].sum == 0)
                                {
                                    continue;
                                }
                                size = hist3 -> hist_tuples[b3].sum * sizeof(tuple);
                                if(size < L1_CACHE)
                                {

                                //     printf("\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------QUICKSORT------\n",b3,hist3 -> hist_tuples[b3].sum,size);
                                    if(b3 == hist3 -> num_tuples - 1)
                                    {
                                        Quicksort(rel_new, ps3 -> psum_tuples[b3].position,ps3 -> psum_tuples[b3].position + hist3 -> hist_tuples[b3].sum - 1);
                                    }
                                    else
                                    {
                                        Quicksort(rel_new,ps3 -> psum_tuples[b3].position,ps3 -> psum_tuples[b3 + 1].position -1);
                                    }
                                }
                                else    // SPLIT
                                {
                                //     printf("\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------SPLIT------\n",b3,hist3 -> hist_tuples[b3].sum,size);
                                    histogram struct_h4;
                                    histogram *hist4 = &struct_h4;
                                    psum struct_p4;
                                    psum *ps4 = &struct_p4;
                                    if(b3 == hist3 -> num_tuples - 1)
                                    {
                                        Split_Bucket(rel_old,hist4,ps4,rel_new,ps3->psum_tuples[b3].position, ps3 -> psum_tuples[b3].position + hist3 -> hist_tuples[b3].sum,3);
                                    }
                                    else
                                    {
                                        Split_Bucket(rel_old,hist4,ps4,rel_new,ps3->psum_tuples[b3].position,ps3->psum_tuples[b3 + 1].position,3);
                                    }

                                    // BYTE 4
                                    for(uint64_t b4 = 0; b4 < hist4 -> num_tuples; b4++)
                                    {
                                        if(hist4 -> hist_tuples[b4].sum == 0)
                                        {
                                            continue;
                                        }
                                        size = hist4 -> hist_tuples[b4].sum * sizeof(tuple);

                                        if(size < L1_CACHE)
                                        {

                                        //     printf("\t\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------QUICKSORT------\n",b4,hist4 -> hist_tuples[b4].sum,size);
                                            if(b4 == hist4 -> num_tuples - 1)
                                            {
                                                Quicksort(rel_new, ps4 -> psum_tuples[b4].position,ps4 -> psum_tuples[b4].position + hist4 -> hist_tuples[b4].sum - 1);
                                            }
                                            else
                                            {
                                                Quicksort(rel_new,ps4 -> psum_tuples[b4].position,ps4 -> psum_tuples[b4 + 1].position -1);
                                            }
                                        }
                                        else    // SPLIT
                                        {
                                        //     printf("\t\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------SPLIT------\n",b4,hist4 -> hist_tuples[b4].sum,size);
                                            histogram struct_h5;
                                            histogram *hist5 = &struct_h5;
                                            psum struct_p5;
                                            psum *ps5 = &struct_p5;

                                            //Print_Relation(rel_new,hist4,ps4);
                                            if(b4 == hist4 -> num_tuples - 1)
                                            {
                                                Split_Bucket(rel_old,hist5,ps5,rel_new,ps4->psum_tuples[b4].position, ps4 -> psum_tuples[b4].position + hist4 -> hist_tuples[b4].sum,2);
                                            }
                                            else
                                            {
                                                Split_Bucket(rel_old,hist5,ps5,rel_new,ps4->psum_tuples[b4].position,ps4->psum_tuples[b4 + 1].position,2);
                                            }

                                            //Print_Relation(rel_new,hist5,ps5);
                                            // BYTE 5
                                            for(uint64_t b5 = 0; b5 < hist5 -> num_tuples; b5++)
                                            {
                                                if(hist5 -> hist_tuples[b5].sum == 0)
                                                {
                                                    continue;
                                                }
                                                size = hist5 -> hist_tuples[b5].sum * sizeof(tuple);

                                                if(size < L1_CACHE)
                                                {


                                                //     printf("\t\t\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------QUICKSORT------\n",b5,hist5 -> hist_tuples[b5].sum,size);
                                                    if(b5 == hist5 -> num_tuples - 1)
                                                    {
                                                        Quicksort(rel_new, ps5 -> psum_tuples[b5].position,ps5 -> psum_tuples[b5].position + hist5 -> hist_tuples[b5].sum - 1);
                                                    }
                                                    else
                                                    {
                                                        Quicksort(rel_new,ps5 -> psum_tuples[b5].position,ps5 -> psum_tuples[b5 + 1].position -1);
                                                    }
                                                }
                                                else    // SPLIT
                                                {
                                                //     printf("\t\t\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------SPLIT------123\n",b5,hist5 -> hist_tuples[b5].sum,size);
                                                    histogram struct_h6;
                                                    histogram *hist6 = &struct_h6;
                                                    psum struct_p6;
                                                    psum *ps6 = &struct_p6;

                                                //    Print_Relation_2(rel_new);
                                                    if(b5 == hist5 -> num_tuples - 1)
                                                    {
                                                        Split_Bucket(rel_old,hist6,ps6,rel_new,ps5->psum_tuples[b5].position, ps5 -> psum_tuples[b5].position + hist5 -> hist_tuples[b5].sum,1);

                                                    }
                                                    else
                                                    {

                                                        Split_Bucket(rel_old,hist6,ps6,rel_new,ps5->psum_tuples[b5].position,ps5->psum_tuples[b5 + 1].position,1);

                                                    }
                                                    //Print_Relation(rel_new,hist6,ps6);
                                                    // BYTE 6
                                                    for(uint64_t b6 = 0; b6 < hist6 -> num_tuples; b6++)
                                                    {
                                                        if(hist6 -> hist_tuples[b6].sum == 0)
                                                        {
                                                            continue;
                                                        }
                                                        size = hist6 -> hist_tuples[b6].sum * sizeof(tuple);

                                                        if(size < L1_CACHE)
                                                        {

                                                        //     printf("\t\t\t\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------QUICKSORT------\n",b6,hist6 -> hist_tuples[b6].sum,size);
                                                            if(b6 == hist6 -> num_tuples - 1)
                                                            {
                                                                Quicksort(rel_new, ps6 -> psum_tuples[b6].position,ps6 -> psum_tuples[b6].position + hist6 -> hist_tuples[b6].sum - 1);
                                                            }
                                                            else
                                                            {
                                                                Quicksort(rel_new,ps6 -> psum_tuples[b6].position,ps6 -> psum_tuples[b6 + 1].position -1);
                                                            }
                                                        }
                                                        else    // SPLIT
                                                        {
                                                        //     printf("\t\t\t\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------SPLIT------\n",b6,hist6 -> hist_tuples[b6].sum,size);
                                                            histogram struct_h7;
                                                            histogram *hist7 = &struct_h7;
                                                            psum struct_p7;
                                                            psum *ps7 = &struct_p7;
                                                            if(b6 == hist6 -> num_tuples - 1)
                                                            {
                                                                Split_Bucket(rel_old,hist7,ps7,rel_new,ps6->psum_tuples[b6].position, ps6 -> psum_tuples[b6].position + hist6 -> hist_tuples[b6].sum,0);
                                                            }
                                                            else
                                                            {
                                                                Split_Bucket(rel_old,hist7,ps7,rel_new,ps6->psum_tuples[b6].position,ps6->psum_tuples[b6 + 1].position,0);
                                                            }

                                                            // BYTE 7
                                                            for(uint64_t b7 = 0; b7 < hist7 -> num_tuples; b7++)
                                                            {
                                                                if(hist7 -> hist_tuples[b7].sum == 0)
                                                                {
                                                                    continue;
                                                                }
                                                                size = hist7 -> hist_tuples[b7].sum * sizeof(tuple);

                                                                if(size < L1_CACHE)
                                                                {

                                                                //     printf("\t\t\t\t\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------QUICKSORT------\n",b7,hist7 -> hist_tuples[b7].sum,size);

                                                                    if(b7 == hist7 -> num_tuples - 1)
                                                                    {
                                                                        Quicksort(rel_new, ps7 -> psum_tuples[b7].position,ps7 -> psum_tuples[b7].position + hist7 -> hist_tuples[b7].sum - 1);
                                                                    }
                                                                    else
                                                                    {
                                                                        Quicksort(rel_new,ps7 -> psum_tuples[b7].position,ps7 -> psum_tuples[b7 + 1].position -1);
                                                                    }
                                                                }
                                                                else    // SPLIT
                                                                {
                                                                //     printf("\t\t\t\t\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------SPLIT------\n",b7,hist7 -> hist_tuples[b7].sum,size);
                                                                    histogram struct_h8;
                                                                    histogram *hist8 = &struct_h8;
                                                                    psum struct_p8;
                                                                    psum *ps8 = &struct_p8;
                                                                    if(b7 == hist7 -> num_tuples - 1)
                                                                    {
                                                                        Split_Bucket(rel_old,hist8,ps8,rel_new,ps7->psum_tuples[b7].position, ps7 -> psum_tuples[b7].position + hist7 -> hist_tuples[b7].sum,-1);
                                                                    }
                                                                    else
                                                                    {
                                                                        Split_Bucket(rel_old,hist8,ps8,rel_new,ps7->psum_tuples[b7].position,ps7->psum_tuples[b7 + 1].position,-1);
                                                                    }

                                                                    // LAST QUICKSORT
                                                                    for(uint64_t b8 = 0; b8 < hist8 -> num_tuples; b8++)
                                                                    {
                                                                        if(hist8 -> hist_tuples[b8].sum == 0)
                                                                        {
                                                                            continue;
                                                                        }
                                                                        size = hist8 -> hist_tuples[b8].sum * sizeof(tuple);
                                                                        if(size < L1_CACHE)
                                                                        {

                                                                        //     printf("\t\t\t\t\t\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------QUICKSORT------\n",b8,hist8 -> hist_tuples[b8].sum,size);
                                                                            if(b8 == hist8 -> num_tuples - 1)
                                                                            {
                                                                                Quicksort(rel_new, ps8 -> psum_tuples[b8].position,ps8 -> psum_tuples[b8].position + hist8 -> hist_tuples[b8].sum - 1);
                                                                            }
                                                                            else
                                                                            {
                                                                                Quicksort(rel_new,ps8 -> psum_tuples[b8].position,ps8 -> psum_tuples[b8 + 1].position -1);
                                                                            }
                                                                        }
                                                                    }
                                                                    free(hist8->hist_tuples);
                                                                    free(ps8->psum_tuples);
                                                                }

                                                            }
                                                            free(hist7->hist_tuples);
                                                            free(ps7->psum_tuples);
                                                        }

                                                    }
                                                    free(hist6->hist_tuples);
                                                    free(ps6->psum_tuples);
                                                }

                                            }
                                            free(hist5->hist_tuples);
                                            free(ps5->psum_tuples);
                                        }

                                    }
                                    free(hist4->hist_tuples);
                                    free(ps4->psum_tuples);
                                }
                            }
                            free(hist3->hist_tuples);
                            free(ps3->psum_tuples);
                        }
                    }
                    free(hist2->hist_tuples);
                    free(ps2->psum_tuples);
                }
            }
            free(hist1->hist_tuples);
            free(ps1->psum_tuples);
        }
        i++;
    }

}



void Print_Relation(relation * rel, histogram * hist, psum * ps)
{
    for (uint64_t i = 0; i < ps -> num_tuples; i++)
    {
        if(hist -> hist_tuples[i].sum == 0)
        {
            continue;
        }
        printf("------ Bucket : %lu -------\n",i);
        if(i == ps -> num_tuples - 1)
        {
            for(uint64_t j = ps -> psum_tuples[i].position; j < ps -> psum_tuples[i].position + hist -> hist_tuples[i].sum; j++)
            {
                printf("%lu) key = %lu|",j, rel->tuples[j].key);
                for (size_t k = 0; k < rel -> tuples[j].position; k++)
                {

                    printf("%lu|",rel->tuples[j].payload[k]);
                }
                printf("\n");
            }
            printf("------------------------------\n");
            break;
        }
        for(uint64_t j = ps -> psum_tuples[i].position; j < ps -> psum_tuples[i + 1].position; j++)
        {
            //printf("%lu key = %lu,  payload = %lu\n",j, rel->tuples[j].key, rel->tuples[j].payload);
            printf("%lu) key = %lu|",j, rel->tuples[j].key);
            for (size_t k = 0; k < rel -> tuples[j].position; k++)
            {

                printf("%lu|",rel->tuples[j].payload[k]);
            }
            printf("\n");
        }
        printf("------------------------------\n");

    }
}

relation * Job_Radix_Sort(relation * rel, job_scheduler * scheduler)
{

    relation * rel_final;
    if(((rel_final = (relation *)malloc(sizeof(relation)))) == NULL)
    {
        perror("Create_Relation malloc");
        exit(-1);
    }

    psum struct_p;
    psum *ps = &struct_p;

    histogram struct_hist;
    histogram * hist_final = &struct_hist;
    /* --------------------------- HISTOGRAM ----------------------------- */
    Hist_Job_Partitions(rel, hist_final,scheduler);
    // scheduler -> jobs_left += scheduler -> total_threads;
    // histogram ** hist = (histogram **)malloc(sizeof(histogram *) * scheduler -> total_threads);
    // for (size_t i = 0; i < scheduler -> total_threads; i++)
    // {
    //     hist[i] = malloc(sizeof(histogram));
    // }
    //
    //
    // uint64_t tuples_per_thread = (rel -> num_tuples) / scheduler -> total_threads;
    // uint64_t tuples_per_thread_final =  (rel -> num_tuples) / scheduler -> total_threads;
    //
    //
    // for (size_t j = 0; j < scheduler -> total_threads; j++)
    // {
    //     job_hist * job_arguments = malloc(sizeof(job_hist));
    //     job_arguments -> rel = rel;
    //     job_arguments -> hist = hist[j];
    //     job_arguments -> sel_byte = 7;
    //     job_arguments -> start = j * tuples_per_thread;
    //     if(j + 1 == scheduler -> total_threads)
    //     {
    //         job_arguments -> end = rel -> num_tuples;
    //     }
    //     else
    //     {
    //        job_arguments -> end = (j + 1) * tuples_per_thread;
    //     }
    //     Assign_Job(scheduler, &JobHist, (void*)job_arguments);
    // }
    //
    // // WAIT FOR ALL PARTS
    // Barrier(scheduler);
    //
    // /* -----Create Final Histogram , by merging partitions------ */
    // uint64_t num_tuples = 0;
    // for (size_t i = 0; i < scheduler -> total_threads; i++)
    // {
    //     num_tuples += Histogram_Tuples(hist[i]);
    //
    // }
    // num_tuples = num_tuples/scheduler -> total_threads;
    // histogram * hist_final = (histogram *)malloc(sizeof(histogram));
    //
    // uint64_t result = 0;
    // uint64_t sel_byte = 7;
    // if((hist_final -> hist_tuples = (hist_tuple *)malloc(num_tuples * sizeof(hist_tuple))) == NULL)
    // {
    //     perror("histogram, first malloc\n");
    //     exit(-1);
    // }
    // hist_final -> num_tuples = num_tuples;
    // for (size_t i = 0; i < num_tuples; i++)
    // {
    //     hist_final -> hist_tuples[result].sum = 0;
    // }
    // for (size_t i = 0; i <  scheduler -> total_threads; i++)
    // {
    //     for (size_t j = 0; j < hist[i] -> num_tuples; j++)
    //     {
    //         if(hist[i] -> hist_tuples[j].sum != 0)
    //         {
    //             result = (hist[i] -> hist_tuples[j].key >> (8*sel_byte) & 0xFF);
    //             hist_final -> hist_tuples[result].sum += hist[i] -> hist_tuples[result].sum;
    //             hist_final -> hist_tuples[result].key =  hist[i] -> hist_tuples[result].key;
    //             hist_final -> hist_tuples[result].payload = hist[i] -> hist_tuples[result].payload;
    //         }
    //     }
    //
    // }
    // for (size_t i = 0; i < scheduler -> total_threads; i++)
    // {
    //     free(hist[i]);
    // }
    // free(hist);
    /* --------- End of Histogram -------- */


    // Print_Histogram(hist_final);

    // Print_Histogram(hist_final);
    Psum(hist_final,ps,0);
    //Print_Psum(hist_final,ps);

    ReorderedColumn(rel,rel_final,ps);

    RestorePsum(hist_final, ps);

    ProcessRelation(rel,hist_final,ps,rel_final,6);

    free(hist_final -> hist_tuples);

    // free(hist_final);
    free(ps -> psum_tuples);
    for(size_t i = 0; i < rel -> num_tuples; i++)
    {
        free(rel->tuples[i].payload);
    }
    free(rel -> tuples);
    free(rel);
    return rel_final;

}



//relation * Radix_Sort(relation * rel)
relation * Radix_Sort(relation * rel)
{

    relation * rel_final;
    if(((rel_final = (relation *)malloc(sizeof(relation)))) == NULL)
    {
        perror("Create_Relation malloc");
        exit(-1);
    }

    histogram struct_h;
    histogram * hist = &struct_h;

    psum struct_p;
    psum *ps = &struct_p;

    Histogram(rel,hist,7,0,rel -> num_tuples);
    //Print_Histogram(hist);

    Psum(hist,ps,0);
    //Print_Psum(hist,ps);

    ReorderedColumn(rel,rel_final,ps);

    RestorePsum(hist, ps);

    ProcessRelation(rel,hist,ps,rel_final,6);

    free(hist -> hist_tuples);
    free(ps -> psum_tuples);
    for(size_t i = 0; i < rel -> num_tuples; i++)
    {
        free(rel->tuples[i].payload);
    }
    free(rel -> tuples);
    free(rel);
    return rel_final;

}

/////////////////////////////////////////////////////////////// PROJECT 2 /////////////////////////////////////////


relation * Create_Relation(metadata * md, uint64_t md_pos,uint64_t array_pos)
{
    uint64_t * ptr;
    //relation rel_struct;
    relation * rel;// = &rel_struct;
    //;

    if(((rel = (relation *)malloc(sizeof(relation)))) == NULL)
    {
        perror("Create_Relation malloc");
        exit(-1);
    }

    // We'll create space for rows number of tuples
    if((rel -> tuples = (tuple *)malloc(md[md_pos].num_tuples * sizeof(tuple))) == NULL)
    {
        perror("Create_Relation.c , first malloc\n");
        exit(-1);
    }

    ptr =  (uint64_t *)(md[md_pos].array[array_pos]);
    for(size_t i = 0; i < md[md_pos].num_tuples; i++)
    {
        rel -> tuples[i].key = *(ptr); // key is the value
        // printf("->%lu\n",rel -> tuples[i].key);

        if((rel -> tuples[i].payload = (uint64_t *)malloc(sizeof(uint64_t))) == NULL)
        {
            perror("Create_Relation.c , first malloc\n");
            exit(-1);
        }
        rel -> tuples[i].payload[0] = i;// payload
        rel -> tuples[i].position = 1;
        ptr++;
    }

    // The number of tuples is the number of rows
    rel -> num_tuples = md[md_pos].num_tuples;

    return rel;
}

void Print_Relation_2(relation * rel)
{
    for (uint64_t i = 0; i < rel -> num_tuples; i++)
    {
        for(uint64_t j = 0; j < rel -> tuples[i].position; j++)
        {
            if(j == 0)
            {
                printf("key = %lu", rel->tuples[i].key);
            }
            printf("\tpayload = %lu",rel->tuples[i].payload[j]);
        }
        printf("\tposition= %lu\n",rel->tuples[i].position);
    }
}

void Update_Tuple_Payload(relation * rel, uint64_t pos, uint64_t key, uint64_t payload)
{
    printf("%lu, %lu\n",payload, rel -> tuples[pos].position);
    if((rel -> tuples[pos].payload = (uint64_t *)realloc(rel -> tuples[pos].payload,(rel -> tuples[pos].position+1) * sizeof(uint64_t))) == NULL)
    {
        perror("Update realloc\n");
        exit(-1);
    }
    //printf("%lu\n",payload);
    rel -> tuples[pos].key = key;
    rel -> tuples[pos].payload[rel -> tuples[pos].position] = payload;
    rel -> tuples[pos].position++;
    printf("%lu, %lu\n",payload, rel -> tuples[pos].position);

}

void Update_Relation_Keys(metadata * md, uint64_t md_row, uint64_t md_column, relation * rel, uint64_t pos)
{
    uint64_t * ptr = (uint64_t *)md[md_row].array[md_column];
    for (uint64_t i = 0; i < rel -> num_tuples; i++)
    {
        rel -> tuples[i].key = *(ptr + rel -> tuples[i].payload[pos]);
    }
}


relation * Update_Predicates(relation * final_rel, uint64_t column)
{
    //relation rel_struct;
    relation * rel;// = &rel_struct;
    //;

    if(((rel = (relation *)malloc(sizeof(relation)))) == NULL)
    {
        perror("Create_Relation malloc");
        exit(-1);
    }

    // We'll create space for rows number of tuples
    if((rel -> tuples = (tuple *)malloc(final_rel -> num_tuples * sizeof(tuple))) == NULL)
    {
        perror("Create_Relation.c , first malloc\n");
        exit(-1);
    }

    //ptr =  md[md_pos].array[array_pos];

    for(size_t i = 0; i < final_rel -> num_tuples; i++)
    {
        rel -> tuples[i].key = final_rel -> tuples[i].key; // key is the value
        if((rel -> tuples[i].payload = (uint64_t *)malloc(sizeof(uint64_t))) == NULL)
        {
            perror("Create_Relation.c , first malloc\n");
            exit(-1);
        }
        rel -> tuples[i].payload[0] = final_rel -> tuples[i].payload[column];// payload
        rel -> tuples[i].position = 1;

    }

    // The number of tuples is the number of rows
    rel -> num_tuples = final_rel -> num_tuples;

    return rel;
}

relation * Update_Interv(relation * final_rel)
{
    //relation rel_struct;
    relation * rel;// = &rel_struct;
    //;

    if(((rel = (relation *)malloc(sizeof(relation)))) == NULL)
    {
        perror("Create_Relation malloc");
        exit(-1);
    }

    // We'll create space for rows number of tuples
    if((rel -> tuples = (tuple *)malloc(final_rel -> num_tuples * sizeof(tuple))) == NULL)
    {
        perror("Create_Relation.c , first malloc\n");
        exit(-1);
    }

    //ptr =  md[md_pos].array[array_pos];

    for(size_t i = 0; i < final_rel -> num_tuples; i++)
    {
        rel -> tuples[i].key = final_rel -> tuples[i].key; // key is the value
        if((rel -> tuples[i].payload = (uint64_t *)malloc(sizeof(uint64_t)* final_rel -> tuples[i].position)) == NULL)
        {
            perror("Create_Relation.c , first malloc\n");
            exit(-1);
        }
        for (size_t j = 0; j < final_rel -> tuples[i].position; j++)
        {
            rel -> tuples[i].payload[j] = final_rel -> tuples[i].payload[j];// payload

        }
        rel -> tuples[i].position = final_rel -> tuples[i].position;

    }

    // The number of tuples is the number of rows
    rel -> num_tuples = final_rel -> num_tuples;

    for(size_t i = 0; i < final_rel -> num_tuples; i++)
	{
		free(final_rel->tuples[i].payload);
	}
	free(final_rel -> tuples);
	free(final_rel);
    return rel;
}



relation * Filter(relation * rel, uint64_t limit, char symbol)
{
    uint64_t counter = 0;
    uint64_t flag = 0;

    uint64_t j = 0;

    // relation struct_A;
	// relation * rel_final = &struct_A;
    relation * rel_final = (relation *)malloc(sizeof(relation));


    for (size_t i = 0; i < rel -> num_tuples; i++)
    {
        //printf("%lu\n", rel -> num_tuples);
        if(symbol == '>')
        {

            if(rel -> tuples[i].key < limit)
            {
                continue;
            }
            else if(rel -> tuples[i].key > limit)
            {
                if(flag == 0)
                {

                    if((rel_final -> tuples = (tuple *)malloc((counter+1) * sizeof(tuple))) == NULL)
                    {
                        perror("Filter , first malloc\n");
                        exit(-1);
                    }
                    flag = 1;

                    rel_final -> tuples[j].key = rel -> tuples[i].key;

                    if((rel_final -> tuples[j].payload = (uint64_t *)malloc((rel -> tuples[i].position) * sizeof(uint64_t))) == NULL)
                    {
                        perror("Filter , second malloc\n");
                        exit(-1);
                    }
                    for (size_t z = 0; z < rel -> tuples[i].position; z++)
                    {
                        rel_final -> tuples[j].payload[z] = rel -> tuples[i].payload[z];

                    }
                    //rel_final -> tuples[j].payload = rel -> tuples[i].payload;
                    rel_final -> num_tuples = 1;
                    rel_final -> tuples[j].position = rel -> tuples[i].position;
                    j++;
                    counter++;

                    continue;
                }
                if((rel_final -> tuples = (tuple *)realloc(rel_final -> tuples,(counter+1) * sizeof(tuple))) == NULL)
                {
                    perror("Filter , realloc\n");
                    exit(-1);
                }
                rel_final -> tuples[j].key = rel -> tuples[i].key;

                if((rel_final -> tuples[j].payload = (uint64_t *)malloc((rel -> tuples[i].position) * sizeof(uint64_t)) ) == NULL)
                {
                    perror("Filter , third malloc\n");
                    exit(-1);
                }
                for (size_t z = 0; z < rel -> tuples[i].position; z++)
                {
                    rel_final -> tuples[j].payload[z] = rel -> tuples[i].payload[z];

                }
                //rel_final -> tuples[j].payload = rel -> tuples[i].payload;
                rel_final -> tuples[j].position = rel -> tuples[i].position;
                rel_final -> num_tuples++;
                j++;
                counter++;
            }
        }
        else if(symbol == '<')
        {
            if(rel -> tuples[i].key > limit)
            {
                continue;
            }
            else if(rel -> tuples[i].key < limit)   // equal?
            {
                if(flag == 0)
                {

                    if((rel_final -> tuples = (tuple *)malloc((counter+1) * sizeof(tuple))) == NULL)
                    {
                        perror("Filter , first malloc\n");
                        exit(-1);
                    }
                    flag = 1;

                    rel_final -> tuples[j].key = rel -> tuples[i].key;

                    if((rel_final -> tuples[j].payload = (uint64_t *)malloc((rel -> tuples[i].position) * sizeof(uint64_t))) == NULL)
                    {
                        perror("Filter , second malloc\n");
                        exit(-1);
                    }
                    for (size_t z = 0; z < rel -> tuples[i].position; z++)
                    {
                        rel_final -> tuples[j].payload[z] = rel -> tuples[i].payload[z];

                    }
                    //rel_final -> tuples[j].payload = rel -> tuples[i].payload;
                    rel_final -> num_tuples = 1;
                    rel_final -> tuples[j].position = rel -> tuples[i].position;
                    j++;
                    counter++;

                    continue;
                }
                if((rel_final -> tuples = (tuple *)realloc(rel_final -> tuples,(counter+1) * sizeof(tuple))) == NULL)
                {
                    perror("Filter , realloc\n");
                    exit(-1);
                }
                rel_final -> tuples[j].key = rel -> tuples[i].key;

                if((rel_final -> tuples[j].payload = (uint64_t *)malloc((rel -> tuples[i].position) * sizeof(uint64_t)) ) == NULL)
                {
                    perror("Filter , third malloc\n");
                    exit(-1);
                }
                for (size_t z = 0; z < rel -> tuples[i].position; z++)
                {
                    rel_final -> tuples[j].payload[z] = rel -> tuples[i].payload[z];

                }
                //rel_final -> tuples[j].payload = rel -> tuples[i].payload;
                rel_final -> tuples[j].position = rel -> tuples[i].position;
                rel_final -> num_tuples++;
                j++;
                counter++;
            }
        }
        else if(symbol == '=')
        {
            if(rel -> tuples[i].key < limit)
            {
                continue;
            }
            else if(rel -> tuples[i].key == limit)   // equal?
            {
                if(flag == 0)
                {

                    if((rel_final -> tuples = (tuple *)malloc((counter+1) * sizeof(tuple))) == NULL)
                    {
                        perror("Filter , first malloc\n");
                        exit(-1);
                    }
                    flag = 1;

                    rel_final -> tuples[j].key = rel -> tuples[i].key;

                    if((rel_final -> tuples[j].payload = (uint64_t *)malloc((rel -> tuples[i].position) * sizeof(uint64_t))) == NULL)
                    {
                        perror("Filter , second malloc\n");
                        exit(-1);
                    }
                    for (size_t z = 0; z < rel -> tuples[i].position; z++)
                    {
                        rel_final -> tuples[j].payload[z] = rel -> tuples[i].payload[z];

                    }
                    //rel_final -> tuples[j].payload = rel -> tuples[i].payload;
                    rel_final -> num_tuples = 1;
                    rel_final -> tuples[j].position = rel -> tuples[i].position;
                    j++;
                    counter++;

                    continue;
                }
                if((rel_final -> tuples = (tuple *)realloc(rel_final -> tuples,(counter+1) * sizeof(tuple))) == NULL)
                {
                    perror("Filter , realloc\n");
                    exit(-1);
                }
                rel_final -> tuples[j].key = rel -> tuples[i].key;

                if((rel_final -> tuples[j].payload = (uint64_t *)malloc((rel -> tuples[i].position) * sizeof(uint64_t)) ) == NULL)
                {
                    perror("Filter , third malloc\n");
                    exit(-1);
                }
                for (size_t z = 0; z < rel -> tuples[i].position; z++)
                {
                    rel_final -> tuples[j].payload[z] = rel -> tuples[i].payload[z];

                }
                //rel_final -> tuples[j].payload = rel -> tuples[i].payload;
                rel_final -> tuples[j].position = rel -> tuples[i].position;
                rel_final -> num_tuples++;
                j++;
                counter++;
            }
            else
            {
                break;
            }

        }
    }
    rel_final -> num_tuples = counter;

    for(size_t i = 0; i < rel -> num_tuples; i++)
    {
        free(rel->tuples[i].payload);
    }
    free(rel -> tuples);
    free(rel);

    return rel_final;

}




void Hist_Job_Partitions(relation * rel, histogram * hist_final,job_scheduler * scheduler)
{
    scheduler -> jobs_left += scheduler -> total_threads;
    histogram ** hist = (histogram **)malloc(sizeof(histogram *) * scheduler -> total_threads);
    for (size_t i = 0; i < scheduler -> total_threads; i++)
    {
        hist[i] = malloc(sizeof(histogram));
    }


    uint64_t tuples_per_thread = (rel -> num_tuples) / scheduler -> total_threads;


    for (size_t j = 0; j < scheduler -> total_threads; j++)
    {
        job_hist * job_arguments = malloc(sizeof(job_hist));
        job_arguments -> rel = rel;
        job_arguments -> hist = hist[j];
        job_arguments -> sel_byte = 7;
        job_arguments -> start = j * tuples_per_thread;
        if(j + 1 == scheduler -> total_threads)
        {
            job_arguments -> end = rel -> num_tuples;
        }
        else
        {
           job_arguments -> end = (j + 1) * tuples_per_thread;
        }
        Assign_Job(scheduler, &JobHist, (void*)job_arguments);
    }

    // WAIT FOR ALL PARTS
    Barrier(scheduler);

    /* -----Create Final Histogram , by merging partitions------ */
    uint64_t num_tuples = 0;
    for (size_t i = 0; i < scheduler -> total_threads; i++)
    {
        num_tuples += Histogram_Tuples(hist[i]);

    }
    num_tuples = num_tuples/scheduler -> total_threads;
    // histogram * hist_final = (histogram *)malloc(sizeof(histogram));

    uint64_t result = 0;
    uint64_t sel_byte = 7;
    if((hist_final -> hist_tuples = (hist_tuple *)malloc(num_tuples * sizeof(hist_tuple))) == NULL)
    {
        perror("histogram, first malloc\n");
        exit(-1);
    }
    hist_final -> num_tuples = num_tuples;
    for (size_t i = 0; i < num_tuples; i++)
    {
        hist_final -> hist_tuples[result].sum = 0;
    }
    for (size_t i = 0; i <  scheduler -> total_threads; i++)
    {
        for (size_t j = 0; j < hist[i] -> num_tuples; j++)
        {
            if(hist[i] -> hist_tuples[j].sum != 0)
            {
                result = (hist[i] -> hist_tuples[j].key >> (8*sel_byte) & 0xFF);
                hist_final -> hist_tuples[result].sum += hist[i] -> hist_tuples[result].sum;
                hist_final -> hist_tuples[result].key =  hist[i] -> hist_tuples[result].key;
                hist_final -> hist_tuples[result].payload = hist[i] -> hist_tuples[result].payload;
            }
        }

    }
    for (size_t i = 0; i < scheduler -> total_threads; i++)
    {
        free(hist[i]);
    }
    free(hist);
}
