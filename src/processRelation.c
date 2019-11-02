#include <stdio.h>
#include "../include/processRelation.h"
#include "../include/quicksort.h"

#define L1_CACHE 65536    //  = 64 kb

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
            printf("--------- Bucket : %lu -> %lu elements -> %lu bytes------QUICKSORT------\n",i,hist -> hist_tuples[i].sum,size);
            //printf("\t Quicksort Bucket %lu\n",i);
            if(i == hist -> num_tuples - 1)
            {
               Quicksort(rel_new, ps -> psum_tuples[i].position,ps -> psum_tuples[i].position + hist -> hist_tuples[i].sum - 1);
            //   printBucket(rel_new,ps->psum_tuples[i].position,ps->psum_tuples[i].position + hist -> hist_tuples[i].sum);
            }
            else
            {

               Quicksort(rel_new,ps -> psum_tuples[i].position,ps -> psum_tuples[i + 1].position -1);
            //   printBucket(rel_new,ps->psum_tuples[i].position,ps->psum_tuples[i + 1].position);
            }
        }
        else        // SPLIT
        {
            printf("--------- Bucket : %lu -> %lu elements -> %lu bytes------SPLIT------\n",i,hist -> hist_tuples[i].sum,size);
            histogram struct_h1;
            histogram *hist1 = &struct_h1;
            psum struct_p1;
            psum *ps1 = &struct_p1;
            hist -> hist_tuples[i].splitsCounter = 1;
            // BYTE 0
            if(i == hist -> num_tuples - 1)
            {
                Split_Bucket(rel_old,hist1,ps1,rel_new,ps->psum_tuples[i].position, ps -> psum_tuples[i].position + hist -> hist_tuples[i].sum,sel_byte);
                //printBucket(rel_new,ps1->psum_tuples[i].position,ps1->psum_tuples[i].position + hist1 -> hist_tuples[i].sum);
            }
            else
            {
                Split_Bucket(rel_old,hist1,ps1,rel_new,ps->psum_tuples[i].position,ps->psum_tuples[i + 1].position,sel_byte);
                //printBucket(rel_new,ps1->psum_tuples[i].position,ps1->psum_tuples[i + 1].position);
                //printf("e %ld , %ld\n\t",ps1->psum_tuples[i].position,ps1->psum_tuples[i + 1].position);
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


                    printf("\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------QUICKSORT------\n",b1,hist1 -> hist_tuples[b1].sum,size);
                    if(b1 == hist1 -> num_tuples - 1)
                    {
                        Quicksort(rel_new, ps1 -> psum_tuples[b1].position,ps1 -> psum_tuples[b1].position + hist1 -> hist_tuples[b1].sum - 1);
                    //    printBucket(rel_new,ps1->psum_tuples[b1].position,ps1->psum_tuples[b1].position + hist1 -> hist_tuples[b1].sum);
                    }
                    else
                    {
                        Quicksort(rel_new,ps1 -> psum_tuples[b1].position,ps1 -> psum_tuples[b1 + 1].position -1);
                    //    printBucket(rel_new,ps1->psum_tuples[b1].position,ps1->psum_tuples[b1 + 1].position);
                    }

                }
                else    // SPLIT
                {
                    printf("\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------SPLIT------\n",b1,hist1 -> hist_tuples[b1].sum,size);
                    histogram struct_h2;
                    histogram *hist2 = &struct_h2;
                    psum struct_p2;
                    psum *ps2 = &struct_p2;

                    hist -> hist_tuples[i].splitsCounter = 2;
                    if(b1 == hist1 -> num_tuples - 1)
                    {

                        Split_Bucket(rel_old,hist2,ps2,rel_new,ps1->psum_tuples[b1].position, ps1 -> psum_tuples[b1].position + hist1 -> hist_tuples[b1].sum,2);
                    //    printBucket(rel_new,ps2->psum_tuples[b1].position,ps2 -> psum_tuples[b1].position + hist2 -> hist_tuples[b1].sum);
                    }
                    else
                    {

                        Split_Bucket(rel_old,hist2,ps2,rel_new,ps1->psum_tuples[b1].position,ps1->psum_tuples[b1 + 1].position,2);
                    //    printBucket(rel_new,ps2->psum_tuples[b1].position,ps2 -> psum_tuples[b1 + 1].position);
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

                            printf("\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------QUICKSORT------\n",b2,hist2 -> hist_tuples[b2].sum,size);
                            if(i == hist2 -> num_tuples - 1)
                            {
                                Quicksort(rel_new, ps2 -> psum_tuples[b2].position,ps2 -> psum_tuples[b2].position + hist2 -> hist_tuples[b2].sum - 1);
                            //    printBucket(rel_new,ps2->psum_tuples[b2].position,ps2->psum_tuples[b2].position + hist2 -> hist_tuples[b2].sum);
                            }
                            else
                            {
                                Quicksort(rel_new,ps2 -> psum_tuples[b2].position,ps2 -> psum_tuples[b2 + 1].position -1);
                            //    printBucket(rel_new,ps2->psum_tuples[b2].position,ps2->psum_tuples[b2 + 1].position);
                            }
                        }
                        else    // SPLIT
                        {
                            printf("\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------SPLIT------\n",b2,hist2 -> hist_tuples[b2].sum,size);
                            histogram struct_h3;
                            histogram *hist3 = &struct_h3;
                            psum struct_p3;
                            psum *ps3 = &struct_p3;
                            hist -> hist_tuples[i].splitsCounter = 3;
                            if(b2 == hist2 -> num_tuples - 1)
                            {
                                Split_Bucket(rel_old,hist3,ps3,rel_new,ps2->psum_tuples[b2].position, ps2 -> psum_tuples[b2].position + hist2 -> hist_tuples[b2].sum,3);
                            //    printBucket(rel_new,ps3->psum_tuples[b2].position,ps3 -> psum_tuples[b2].position + hist3 -> hist_tuples[b2].sum);
                            }
                            else
                            {
                                Split_Bucket(rel_old,hist3,ps3,rel_new,ps2->psum_tuples[b2].position,ps2->psum_tuples[b2 + 1].position,3);
                            //    printBucket(rel_new,ps3->psum_tuples[b2].position,ps3 -> psum_tuples[b2 + 1].position);
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

                                    printf("\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------QUICKSORT------\n",b3,hist3 -> hist_tuples[b3].sum,size);
                                    if(i == hist3 -> num_tuples - 1)
                                    {
                                        Quicksort(rel_new, ps3 -> psum_tuples[b3].position,ps3 -> psum_tuples[b3].position + hist3 -> hist_tuples[b3].sum - 1);
                                    //    printBucket(rel_new,ps3->psum_tuples[b3].position,ps3->psum_tuples[b3].position + hist3 -> hist_tuples[b3].sum);
                                    }
                                    else
                                    {
                                        Quicksort(rel_new,ps3 -> psum_tuples[b3].position,ps3 -> psum_tuples[b3 + 1].position -1);
                                    //    printBucket(rel_new,ps3->psum_tuples[b3].position,ps3->psum_tuples[b3 + 1].position);
                                    }
                                }
                                else    // SPLIT
                                {
                                    printf("\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------SPLIT------\n",b3,hist3 -> hist_tuples[b3].sum,size);
                                    histogram struct_h4;
                                    histogram *hist4 = &struct_h4;
                                    psum struct_p4;
                                    psum *ps4 = &struct_p4;
                                    hist -> hist_tuples[i].splitsCounter = 4;
                                    if(b3 == hist3 -> num_tuples - 1)
                                    {
                                        Split_Bucket(rel_old,hist4,ps4,rel_new,ps3->psum_tuples[b3].position, ps3 -> psum_tuples[b3].position + hist3 -> hist_tuples[b3].sum,4);
                                    //    printBucket(rel_new,ps4->psum_tuples[b3].position,ps4 -> psum_tuples[b3].position + hist4 -> hist_tuples[b3].sum);
                                    }
                                    else
                                    {
                                        Split_Bucket(rel_old,hist4,ps4,rel_new,ps3->psum_tuples[b3].position,ps3->psum_tuples[b3 + 1].position,4);
                                    //    printBucket(rel_new,ps4->psum_tuples[b3].position,ps4 -> psum_tuples[b3 + 1].position);
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

                                            printf("\t\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------QUICKSORT------\n",b4,hist4 -> hist_tuples[b4].sum,size);
                                            if(i == hist4 -> num_tuples - 1)
                                            {
                                                Quicksort(rel_new, ps4 -> psum_tuples[b4].position,ps3 -> psum_tuples[b4].position + hist4 -> hist_tuples[b4].sum - 1);
                                            //    printBucket(rel_new,ps4->psum_tuples[b4].position,ps4->psum_tuples[b4].position + hist4 -> hist_tuples[b4].sum);
                                            }
                                            else
                                            {
                                                Quicksort(rel_new,ps4 -> psum_tuples[b4].position,ps3 -> psum_tuples[b4 + 1].position -1);
                                            //    printBucket(rel_new,ps4->psum_tuples[b4].position,ps4->psum_tuples[b4 + 1].position);
                                            }
                                        }
                                        else    // SPLIT
                                        {
                                            printf("\t\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------SPLIT------\n",b4,hist4 -> hist_tuples[b4].sum,size);
                                            histogram struct_h5;
                                            histogram *hist5 = &struct_h5;
                                            psum struct_p5;
                                            psum *ps5 = &struct_p5;
                                            hist -> hist_tuples[i].splitsCounter = 5;
                                            if(b4 == hist4 -> num_tuples - 1)
                                            {
                                                Split_Bucket(rel_old,hist5,ps5,rel_new,ps4->psum_tuples[b4].position, ps4 -> psum_tuples[b4].position + hist4 -> hist_tuples[b4].sum,5);
                                            //    printBucket(rel_new,ps5->psum_tuples[b4].position,ps5 -> psum_tuples[b4].position + hist4 -> hist_tuples[b4].sum);
                                            }
                                            else
                                            {
                                                Split_Bucket(rel_old,hist5,ps5,rel_new,ps4->psum_tuples[b4].position,ps4->psum_tuples[b4 + 1].position,5);
                                            //    printBucket(rel_new,ps5->psum_tuples[b4].position,ps5 -> psum_tuples[b4 + 1].position);
                                            }

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


                                                    printf("\t\t\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------QUICKSORT------\n",b5,hist5 -> hist_tuples[b5].sum,size);
                                                    if(i == hist5 -> num_tuples - 1)
                                                    {
                                                        Quicksort(rel_new, ps5 -> psum_tuples[b5].position,ps3 -> psum_tuples[b5].position + hist5 -> hist_tuples[b5].sum - 1);
                                                    //    printBucket(rel_new,ps5->psum_tuples[b5].position,ps5->psum_tuples[b5].position + hist5 -> hist_tuples[b5].sum);
                                                    }
                                                    else
                                                    {
                                                        Quicksort(rel_new,ps5 -> psum_tuples[b5].position,ps5 -> psum_tuples[b5 + 1].position -1);
                                                    //    printBucket(rel_new,ps5->psum_tuples[b5].position,ps5->psum_tuples[b5 + 1].position);
                                                    }
                                                }
                                                else    // SPLIT
                                                {
                                                    printf("\t\t\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------SPLIT------\n",b5,hist5 -> hist_tuples[b5].sum,size);
                                                    histogram struct_h6;
                                                    histogram *hist6 = &struct_h6;
                                                    psum struct_p6;
                                                    psum *ps6 = &struct_p6;
                                                    hist -> hist_tuples[i].splitsCounter = 6;
                                                    if(b5 == hist5 -> num_tuples - 1)
                                                    {
                                                        Split_Bucket(rel_old,hist6,ps6,rel_new,ps5->psum_tuples[b5].position, ps5 -> psum_tuples[b5].position + hist5 -> hist_tuples[b5].sum,6);
                                                    //    printBucket(rel_new,ps6->psum_tuples[b5].position,ps6 -> psum_tuples[b5].position + hist5 -> hist_tuples[b5].sum);
                                                    }
                                                    else
                                                    {
                                                        Split_Bucket(rel_old,hist6,ps6,rel_new,ps5->psum_tuples[b5].position,ps5->psum_tuples[b5 + 1].position,6);
                                                    //    printBucket(rel_new,ps6->psum_tuples[b5].position,ps6 -> psum_tuples[b5 + 1].position);
                                                    }

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

                                                            printf("\t\t\t\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------QUICKSORT------\n",b6,hist6 -> hist_tuples[b6].sum,size);
                                                            if(i == hist6 -> num_tuples - 1)
                                                            {
                                                                Quicksort(rel_new, ps6 -> psum_tuples[b6].position,ps6 -> psum_tuples[b6].position + hist6 -> hist_tuples[b6].sum - 1);
                                                            //    printBucket(rel_new,ps6->psum_tuples[b6].position,ps6->psum_tuples[b6].position + hist6 -> hist_tuples[b6].sum);
                                                            }
                                                            else
                                                            {
                                                                Quicksort(rel_new,ps6 -> psum_tuples[b6].position,ps6 -> psum_tuples[b6 + 1].position -1);
                                                            //    printBucket(rel_new,ps6->psum_tuples[b6].position,ps6->psum_tuples[b6 + 1].position);
                                                            }
                                                        }
                                                        else    // SPLIT
                                                        {
                                                            printf("\t\t\t\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------SPLIT------\n",b6,hist6 -> hist_tuples[b6].sum,size);
                                                            histogram struct_h7;
                                                            histogram *hist7 = &struct_h7;
                                                            psum struct_p7;
                                                            psum *ps7 = &struct_p7;
                                                            hist -> hist_tuples[i].splitsCounter = 7;
                                                            if(b6 == hist6 -> num_tuples - 1)
                                                            {
                                                                Split_Bucket(rel_old,hist7,ps7,rel_new,ps6->psum_tuples[b6].position, ps6 -> psum_tuples[b6].position + hist6 -> hist_tuples[b6].sum,7);
                                                            //    printBucket(rel_new,ps7->psum_tuples[b6].position,ps7 -> psum_tuples[b6].position + hist7 -> hist_tuples[b6].sum);
                                                            }
                                                            else
                                                            {
                                                                Split_Bucket(rel_old,hist7,ps7,rel_new,ps6->psum_tuples[b6].position,ps6->psum_tuples[b6 + 1].position,7);
                                                            //    printBucket(rel_new,ps7->psum_tuples[b6].position,ps7 -> psum_tuples[b6 + 1].position);
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

                                                                    printf("\t\t\t\t\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------QUICKSORT------\n",b7,hist7 -> hist_tuples[b7].sum,size);

                                                                    if(i == hist7 -> num_tuples - 1)
                                                                    {
                                                                        Quicksort(rel_new, ps7 -> psum_tuples[b7].position,ps7 -> psum_tuples[b7].position + hist7 -> hist_tuples[b7].sum - 1);
                                                                    //    printBucket(rel_new,ps7->psum_tuples[b7].position,ps7->psum_tuples[b7].position + hist7 -> hist_tuples[b7].sum);
                                                                    }
                                                                    else
                                                                    {
                                                                        Quicksort(rel_new,ps7 -> psum_tuples[b7].position,ps7 -> psum_tuples[b7 + 1].position -1);
                                                                    //    printBucket(rel_new,ps7->psum_tuples[b7].position,ps7->psum_tuples[b7 + 1].position);
                                                                    }
                                                                }
                                                                else    // SPLIT
                                                                {
                                                                    printf("\t\t\t\t\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------SPLIT------\n",b7,hist7 -> hist_tuples[b7].sum,size);
                                                                    histogram struct_h8;
                                                                    histogram *hist8 = &struct_h8;
                                                                    psum struct_p8;
                                                                    psum *ps8 = &struct_p8;
                                                                    hist -> hist_tuples[i].splitsCounter = 8;
                                                                    if(b7 == hist7 -> num_tuples - 1)
                                                                    {
                                                                        Split_Bucket(rel_old,hist8,ps8,rel_new,ps7->psum_tuples[b7].position, ps7 -> psum_tuples[b7].position + hist7 -> hist_tuples[b7].sum,8);
                                                                    //    printBucket(rel_new,ps8->psum_tuples[b7].position,ps8 -> psum_tuples[b7].position + hist8 -> hist_tuples[b7].sum);
                                                                    }
                                                                    else
                                                                    {
                                                                        Split_Bucket(rel_old,hist8,ps8,rel_new,ps7->psum_tuples[b7].position,ps7->psum_tuples[b7 + 1].position,8);
                                                                    //    printBucket(rel_new,ps8->psum_tuples[b7].position,ps8 -> psum_tuples[b7 + 1].position);
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

                                                                            printf("\t\t\t\t\t\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------QUICKSORT------\n",b8,hist8 -> hist_tuples[b8].sum,size);
                                                                            if(i == hist8 -> num_tuples - 1)
                                                                            {
                                                                                Quicksort(rel_new, ps8 -> psum_tuples[b8].position,ps8 -> psum_tuples[b8].position + hist8 -> hist_tuples[b8].sum - 1);
                                                                            //    printBucket(rel_new,ps8->psum_tuples[b8].position,ps8->psum_tuples[b8].position + hist8 -> hist_tuples[b8].sum);
                                                                            }
                                                                            else
                                                                            {
                                                                                Quicksort(rel_new,ps8 -> psum_tuples[b8].position,ps8 -> psum_tuples[b8 + 1].position -1);
                                                                            //    printBucket(rel_new,ps8->psum_tuples[b8].position,ps8->psum_tuples[b8 + 1].position);
                                                                            }
                                                                        }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
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
                printf("%lu key = %lu,  payload = %lu\n",j, rel->tuples[j].key, rel->tuples[j].payload);
            }
            printf("------------------------------\n");
            break;
        }
        for(uint64_t j = ps -> psum_tuples[i].position; j < ps -> psum_tuples[i + 1].position; j++)
        {
            printf("%lu key = %lu,  payload = %lu\n",j, rel->tuples[j].key, rel->tuples[j].payload);
        }
        printf("------------------------------\n");

    }
}
