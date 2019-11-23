#include <stdio.h>
#include "../include/processRelation.h"



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
            printf("--------- Bucket : %lu -> %lu elements -> %lu bytes------SPLIT------\n",i,hist -> hist_tuples[i].sum,size);
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


                    printf("\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------QUICKSORT------\n",b1,hist1 -> hist_tuples[b1].sum,size);
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
                    printf("\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------SPLIT------\n",b1,hist1 -> hist_tuples[b1].sum,size);
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

                            printf("\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------QUICKSORT------\n",b2,hist2 -> hist_tuples[b2].sum,size);
                            if(i == hist2 -> num_tuples - 1)
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
                            printf("\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------SPLIT------\n",b2,hist2 -> hist_tuples[b2].sum,size);
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

                                    printf("\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------QUICKSORT------\n",b3,hist3 -> hist_tuples[b3].sum,size);
                                    if(i == hist3 -> num_tuples - 1)
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
                                    printf("\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------SPLIT------\n",b3,hist3 -> hist_tuples[b3].sum,size);
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

                                            printf("\t\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------QUICKSORT------\n",b4,hist4 -> hist_tuples[b4].sum,size);
                                            if(i == hist4 -> num_tuples - 1)
                                            {
                                                Quicksort(rel_new, ps4 -> psum_tuples[b4].position,ps3 -> psum_tuples[b4].position + hist4 -> hist_tuples[b4].sum - 1);
                                            }
                                            else
                                            {
                                                Quicksort(rel_new,ps4 -> psum_tuples[b4].position,ps3 -> psum_tuples[b4 + 1].position -1);
                                            }
                                        }
                                        else    // SPLIT
                                        {
                                            printf("\t\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------SPLIT------\n",b4,hist4 -> hist_tuples[b4].sum,size);
                                            histogram struct_h5;
                                            histogram *hist5 = &struct_h5;
                                            psum struct_p5;
                                            psum *ps5 = &struct_p5;
                                            if(b4 == hist4 -> num_tuples - 1)
                                            {
                                                Split_Bucket(rel_old,hist5,ps5,rel_new,ps4->psum_tuples[b4].position, ps4 -> psum_tuples[b4].position + hist4 -> hist_tuples[b4].sum,2);
                                            }
                                            else
                                            {
                                                Split_Bucket(rel_old,hist5,ps5,rel_new,ps4->psum_tuples[b4].position,ps4->psum_tuples[b4 + 1].position,2);
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
                                                    }
                                                    else
                                                    {
                                                        Quicksort(rel_new,ps5 -> psum_tuples[b5].position,ps5 -> psum_tuples[b5 + 1].position -1);
                                                    }
                                                }
                                                else    // SPLIT
                                                {
                                                    printf("\t\t\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------SPLIT------\n",b5,hist5 -> hist_tuples[b5].sum,size);
                                                    histogram struct_h6;
                                                    histogram *hist6 = &struct_h6;
                                                    psum struct_p6;
                                                    psum *ps6 = &struct_p6;
                                                    if(b5 == hist5 -> num_tuples - 1)
                                                    {
                                                        Split_Bucket(rel_old,hist6,ps6,rel_new,ps5->psum_tuples[b5].position, ps5 -> psum_tuples[b5].position + hist5 -> hist_tuples[b5].sum,1);
                                                    }
                                                    else
                                                    {
                                                        Split_Bucket(rel_old,hist6,ps6,rel_new,ps5->psum_tuples[b5].position,ps5->psum_tuples[b5 + 1].position,1);
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
                                                            }
                                                            else
                                                            {
                                                                Quicksort(rel_new,ps6 -> psum_tuples[b6].position,ps6 -> psum_tuples[b6 + 1].position -1);
                                                            }
                                                        }
                                                        else    // SPLIT
                                                        {
                                                            printf("\t\t\t\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------SPLIT------\n",b6,hist6 -> hist_tuples[b6].sum,size);
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

                                                                    printf("\t\t\t\t\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------QUICKSORT------\n",b7,hist7 -> hist_tuples[b7].sum,size);

                                                                    if(i == hist7 -> num_tuples - 1)
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
                                                                    printf("\t\t\t\t\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------SPLIT------\n",b7,hist7 -> hist_tuples[b7].sum,size);
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

                                                                            printf("\t\t\t\t\t\t\t\t>--------- Bucket : %lu -> %lu elements -> %lu bytes------QUICKSORT------\n",b8,hist8 -> hist_tuples[b8].sum,size);
                                                                            if(i == hist8 -> num_tuples - 1)
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

void Final_Relation(relation * rel, relation * rel_final)
{
    histogram struct_h;
    histogram * hist = &struct_h;

    psum struct_p;
    psum *ps = &struct_p;

    Histogram(rel,hist,7,0,rel -> num_tuples);
    Print_Histogram(hist);

    Psum(hist,ps,0);
    Print_Psum(hist,ps);

    ReorderedColumn(rel,rel_final,ps);

    RestorePsum(hist, ps);

    ProcessRelation(rel,hist,ps,rel_final,6);

    free(hist -> hist_tuples);
    free(ps -> psum_tuples);
    free(rel -> tuples);

}


void Create_Relation(metadata * md, uint64_t md_pos,uint64_t array_pos, relation * rel)
{
    uint64_t * ptr;

    // We'll create space for rows number of tuples
    if((rel -> tuples = (tuple *)malloc(md[md_pos].num_tuples * sizeof(tuple))) == NULL)
    {
        perror("Create_Relation.c , first malloc\n");
        exit(-1);
    }

    ptr =  md[md_pos].array[array_pos];

    for(size_t i = 0; i < md[md_pos].num_tuples; i++)
    {
        rel -> tuples[i].key = *(ptr + i); // key is the value
        rel -> tuples[i].payload = i; // payload

    }

    // The number of tuples is the number of rows
    rel -> num_tuples = md[md_pos].num_tuples;


}
void Filter(relation * rel, uint64_t limit, char symbol, relation * rel_final)
{
    uint64_t counter = 0;
        printf("aa %lu-%c\n",rel -> num_tuples,symbol);
        uint64_t flag = 0;

        uint64_t j = 0;
    for (size_t i = 0; i < rel -> num_tuples; i++)
    {
        if(symbol == '>')
        {

            if(limit > rel -> tuples[i].key)
            {
                continue;
            }
            else if(limit < rel -> tuples[i].key)
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
                    rel_final -> tuples[j].payload = rel -> tuples[i].payload;
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
                rel_final -> tuples[j].payload = rel -> tuples[i].payload;
                j++;
                counter++;
            }
        }
        // else if(strcmp(symbol,"<"))
        // {
        //
        // }
        // else if(strcmp(symbol,"="))
        // {
        //
        // }
    }
    rel_final -> num_tuples = counter;
    printf("counter = %lu\n", counter);

/*
    printf("aa\n");
    // We'll create space for rows number of tuples
    if((rel_final -> tuples = (tuple *)malloc(counter * sizeof(tuple))) == NULL)
    {
        perror("Filter , first malloc\n");
        exit(-1);
    }
    rel_final -> num_tuples = counter;
    uint64_t j = 0;
    for (size_t i = 0; i < rel -> num_tuples; i++)
    {
        if(symbol == '>')
        {
            if(limit > rel -> tuples[i].key)
            {
                continue;
            }
            else if(limit < rel -> tuples[i].key)
            {
                rel_final -> tuples[j].key = rel -> tuples[i].key;
                rel_final -> tuples[j].payload = rel -> tuples[i].payload;
                j++;
            }
        }
        else if(symbol == '<')
        {

        }
        else if(symbol == '=')
        {

        }
    }
*/
    //free(rel);
        printf("counter = %lu\n", counter);

}
