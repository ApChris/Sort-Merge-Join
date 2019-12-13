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
                                            if(i == hist4 -> num_tuples - 1)
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
                                                    if(i == hist5 -> num_tuples - 1)
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
    free(rel -> tuples);

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

    ptr =  md[md_pos].array[array_pos];

    for(size_t i = 0; i < md[md_pos].num_tuples; i++)
    {
        rel -> tuples[i].key = *(ptr + i); // key is the value
        if((rel -> tuples[i].payload = (uint64_t *)malloc(sizeof(uint64_t))) == NULL)
        {
            perror("Create_Relation.c , first malloc\n");
            exit(-1);
        }
        rel -> tuples[i].payload[0] = i;// payload
        rel -> tuples[i].position = 1;

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

void Update_Tuple_Payload(metadata * md, relation * rel, uint64_t pos, uint64_t key, uint64_t payload)
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
    uint64_t * ptr = md[md_row].array[md_column];
    for (uint64_t i = 0; i < rel -> num_tuples; i++)
    {
        rel -> tuples[i].key = *(ptr + rel -> tuples[i].payload[pos]);
    }
}


relation * Update_Predicates(relation * final_rel, uint64_t column)
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
//
// uint64_t CheckSum(metadata * md, uint64_t md_row, uint64_t md_column, relation * rel, uint64_t pos)
// {
//     uint64_t * ptr = md[md_row].array[md_column];
//     uint64_t sum = 0;
//     for (uint64_t i = 0; i < rel -> num_tuples; i++)
//     {
//         sum += *(ptr + rel -> tuples[i].payload[pos]); // calculate keys
//         //sum += rel -> tuples[i].payload[pos]; // calculate payloads
//     }
//     if(sum == 0)
//     {
//         printf("NULL ");
//     }
//     else
//     {
//         printf("%lu ",sum);
//     }
//     return sum;
// }


relation * Filter(relation * rel, uint64_t limit, char symbol)
{
    uint64_t counter = 0;
    uint64_t flag = 0;

    uint64_t j = 0;

    relation struct_A;
	relation * rel_final = &struct_A;
    rel_final = (relation *)malloc(sizeof(relation));


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
        else if(symbol,"=")
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

    rel = rel_final;

//    printf("counter = %lu\n", counter);
    if(counter == 0)
    {
        return NULL;
    }
    return rel_final;

}


// relation * Init_pointer()
// {
//     relation * rel;
//     if((rel = (relation *)malloc(sizeof(relation))) == NULL)
//     {
//         perror("Init pointer error");
//         exit(-1);
//     }
//     return rel;
// }
//
// query_tuple * Init_Query_Tuple()
// {
//     query_tuple * qt;
//     if((qt = (query_tuple *)malloc(sizeof(query_tuple))) == NULL)
//     {
//         perror("Query_Tuple error");
//         exit(-1);
//     }
//     // qt[0].file1_ID = 0;
//     // qt[0].file1_column = 0;
//     // qt[0].rel = NULL;
//     return qt;
// }
//
// uint64_t Find_Query_Tuple(query_tuple * qt, uint64_t file_ID, uint64_t file_column,uint64_t counter)
// {
//     for (size_t i = 0; i < counter; i++)
//     {
//         if((qt[i].file1_ID == file_ID) && (qt[i].file1_column == file_column))
//         {
//             return i;
//         }
//     }
//     return TAG;
// }

//
// void Execute_Queries(metadata * md, work_line * wl_ptr)
// {
//
//     uint64_t rel_counter = 0;
//     uint64_t rel_A_pos = 0;
//     uint64_t rel_B_pos = 0;
//     // For every Query
//     for (uint64_t i = 5; i < wl_ptr -> num_predicates; i++)
//     {
//         if(i == 12 || i == 15 || i == 16|| i == 21)
//         {
//             continue;
//         }
//
//         rel_counter = 0;
//         intervening * interv_final = interveningInit();
//         // variable to count how many rel are we going to use
//         query_tuple * qt;
//
//         // Filters
//         for (uint64_t j = 0; j < wl_ptr -> filters[i].num_tuples; j++)
//         {
//             // First time
//             if(rel_counter == 0)
//             {
//                 // Create Tuple
//                 if((qt = Init_Query_Tuple()) == NULL)
//                 {
//                     perror("Execute_Queries 1st malloc");
//                     exit(-1);
//                 }
//
//                 qt[rel_counter].file1_ID = wl_ptr -> parameters[i].tuples[wl_ptr -> filters[i].tuples[j].file1_ID].file1_ID;
//                 qt[rel_counter].file1_column = wl_ptr -> filters[i].tuples[j].file1_column;
//                 qt[rel_counter].rel = Create_Relation(md,qt[rel_counter].file1_ID,qt[rel_counter].file1_column);
//                 qt[rel_counter].rel = Radix_Sort(qt[rel_counter].rel);
//                 qt[rel_counter].rel = Filter(qt[rel_counter].rel,wl_ptr -> filters[i].tuples[j].limit, wl_ptr -> filters[i].tuples[j].symbol);
//                 interv_final -> final_rel = Filter(qt[rel_counter].rel,wl_ptr -> filters[i].tuples[j].limit, wl_ptr -> filters[i].tuples[j].symbol);
//                 interv_final -> final_rel = 1;
//                 printf("%lu------------------->\n",interv_final -> final_rel);
//                 rel_counter++;
//
//
//
//             }
//             else // if we have more than 1 filter
//             {
//
//                 if(Find_Query_Tuple(qt, wl_ptr -> filters[i].tuples[j].file1_ID, wl_ptr -> filters[i].tuples[j].file1_column, rel_counter) != TAG)
//                 {
//                     printf("Already exists!\n");
//                 }
//                 else
//                 {
//                     if((qt = (query_tuple *)realloc(qt,sizeof(query_tuple) * (rel_counter + 1))) == NULL)
//                     {
//                         perror("Execute_Queries 1st realloc");
//                         exit(-1);
//                     }
//
//                     qt[rel_counter].file1_ID = wl_ptr -> parameters[i].tuples[wl_ptr -> filters[i].tuples[j].file1_ID].file1_ID;
//                     qt[rel_counter].file1_column = wl_ptr -> filters[i].tuples[j].file1_column;
//                     qt[rel_counter].rel = Create_Relation(md,qt[rel_counter].file1_ID,qt[rel_counter].file1_column);
//                     qt[rel_counter].rel = Radix_Sort(qt[rel_counter].rel);
//                     qt[rel_counter].rel = Filter(qt[rel_counter].rel,wl_ptr -> filters[i].tuples[j].limit, wl_ptr -> filters[i].tuples[j].symbol);
//                     interv_final -> final_rel = Filter(qt[rel_counter].rel,wl_ptr -> filters[i].tuples[j].limit, wl_ptr -> filters[i].tuples[j].symbol);
//                     rel_counter++;
//                 }
//
//             }
//         }
//
//         // For predicates
//         for (uint64_t j = 0; j < wl_ptr -> predicates[i].num_tuples; j++)
//         {
//             // if we don't have any filter
//             if(rel_counter == 0)
//             {
//                 if((qt = Init_Query_Tuple()) == NULL)
//                 {
//                     perror("Execute_Queries 2nd malloc");
//                     exit(-1);
//                 }
//                 if((qt = (query_tuple *)realloc(qt,sizeof(query_tuple) * (rel_counter + 1))) == NULL)
//                 {
//                     perror("Execute_Queries 1st realloc");
//                     exit(-1);
//                 }
//
//                 qt[rel_counter].file1_ID = wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID;
//                 qt[rel_counter].file1_column = wl_ptr -> predicates[i].tuples[j].file1_column;
//                 qt[rel_counter].rel = Create_Relation(md,qt[rel_counter].file1_ID,qt[rel_counter].file1_column);
//                 qt[rel_counter].rel = Radix_Sort(qt[rel_counter].rel);
//                 if(Find_Query_Tuple(qt, wl_ptr -> predicates[i].tuples[j].file2_ID, wl_ptr -> predicates[i].tuples[j].file2_column, rel_counter) != TAG)
//                 {
//
//                 }
//                 else
//                 {
//
//                     qt[rel_counter + 1].file1_ID = wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file2_ID].file1_ID;
//                     qt[rel_counter + 1].file1_column = wl_ptr -> predicates[i].tuples[j].file2_column;
//                     qt[rel_counter + 1].rel = Create_Relation(md,qt[rel_counter + 1].file1_ID,qt[rel_counter + 1].file1_column);
//                     qt[rel_counter + 1].rel = Radix_Sort(qt[rel_counter + 1].rel);
//                 }
//
//                 rel_counter+=2;
//             }
//             else        // A filter already exists
//             {
//
//                 // Example: if we have the query: 0.1 = 1.0 & 0.1 > 3000
//                 if(Find_Query_Tuple(qt,wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID, wl_ptr -> predicates[i].tuples[j].file1_column, rel_counter) != TAG)
//                 {
//                 //    printf("Yparxei!!\n");
//
//                 }
//                 // Example: if we have the query: 1.0 = x & 0.1 > 3000
//                 else
//                 {
//                     if((qt = (query_tuple *)realloc(qt,sizeof(query_tuple) * (rel_counter + 1))) == NULL)
//                     {
//                         perror("Execute_Queries 1st realloc");
//                         exit(-1);
//                     }
//                     qt[rel_counter].file1_ID = wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID;
//                     //qt[rel_counter].file1_ID = wl_ptr -> predicates[i].tuples[j].file1_ID;
//                     qt[rel_counter].file1_column = wl_ptr -> predicates[i].tuples[j].file1_column;
//                     qt[rel_counter].rel = Create_Relation(md,qt[rel_counter].file1_ID,qt[rel_counter].file1_column);
//                     qt[rel_counter].rel = Radix_Sort(qt[rel_counter].rel);
//
//                     rel_counter++;
//                 }
//                 // Example: if we have the query: x = 0.1 & 0.1 > 3000
//                 if(Find_Query_Tuple(qt, wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file2_ID].file1_ID, wl_ptr -> predicates[i].tuples[j].file2_column, rel_counter) != TAG)
//                 {
//                 //    printf("Yparxei sth 2h!!\n");
//                 }
//                 // Example: if we have the query: x = 1.0 & 0.1 > 3000
//                 else
//                 {
//                     if((qt = (query_tuple *)realloc(qt,sizeof(query_tuple) * (rel_counter + 1))) == NULL)
//                     {
//                         perror("Execute_Queries 1st realloc");
//                         exit(-1);
//                     }
//                     qt[rel_counter].file1_ID = wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file2_ID].file1_ID;
//                     //qt[rel_counter].file1_ID = wl_ptr -> predicates[i].tuples[j].file2_ID;
//                     qt[rel_counter].file1_column = wl_ptr -> predicates[i].tuples[j].file2_column;
//                     qt[rel_counter].rel = Create_Relation(md,qt[rel_counter].file1_ID,qt[rel_counter].file1_column);
//                     qt[rel_counter].rel = Radix_Sort(qt[rel_counter].rel);
//                 //    printf("mphka2--- %lu.%lu\n",qt[rel_counter].file1_ID,qt[rel_counter].file1_column);
//
//                     rel_counter++;
//                 }
//
//             }
//         }
//
//        // printf("\n\nStarting:%lu\n",rel_counter);
//        //
//        //  for (size_t j = 0; j < rel_counter; j++)
//        //  {
//        //      printf("%lu.%lu   Pos->%lu\n", qt[j].file1_ID,qt[j].file1_column,j);
//        //  }
//        //  printf("\n\n");
//         for (uint64_t j = 0; j < wl_ptr -> predicates[i].num_tuples; j++)
//         {
//             // for first predicate only
//
//             if(j == 0)
//             {
//
//                 for (size_t z = 0; z < rel_counter; z++)
//                 {
//
//                     if(wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID == qt[z].file1_ID)
//                     {
//                         printf("smphka %lu-->%lu\n",i,qt[z].file1_ID);
//                         // Print_Relation_2(interv_final -> final_rel);
//                         // exit(-1);
//                         rel_A_pos = Find_Query_Tuple(qt, wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID,wl_ptr -> predicates[i].tuples[j].file1_column,rel_counter);
//
//                         rel_B_pos = Find_Query_Tuple(qt, wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file2_ID].file1_ID,wl_ptr -> predicates[i].tuples[j].file2_column,rel_counter);
//                         // if(interv_final -> position == 0)
//                         // {
//                         //     printf("amphka %lu-->%lu\n",i,qt[z].file1_ID);
//                         //
//                         // }
//                         // else
//                         // {
//                             Update_Relation_Keys(md,qt[z].file1_ID,wl_ptr -> predicates[i].tuples[j].file1_column,interv_final -> final_rel,z);
//                             interv_final -> final_rel = Radix_Sort(interv_final -> final_rel);
//                             //    printf("j= %lu,z=%lu)%lu.%lu\n",j,  z, qt[rel_B_pos].file1_ID,qt[rel_B_pos].file1_column);
//                             Join_v2(interv_final, interv_final -> final_rel, qt[rel_B_pos].rel, qt[z].file1_ID, wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file2_ID].file1_ID);
//                     //    }
//
//                         printf("dmphka %lu-->%lu\n",i,qt[z].file1_ID);
//
//                         break;
//
//                         //Print_Relation_2(interv_final -> final_rel);
//
//                     }
//
//                     if(wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file2_ID].file1_ID == qt[z].file1_ID)
//                     {
//                         printf("LOOOOOOOOOOOOOOOOOLmphka %lu\n",i);
//                         rel_A_pos = Find_Query_Tuple(qt, wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID,wl_ptr -> predicates[i].tuples[j].file1_column,rel_counter);
//
//                         rel_B_pos = Find_Query_Tuple(qt, wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file2_ID].file1_ID,wl_ptr -> predicates[i].tuples[j].file2_column,rel_counter);
//
//                         Update_Relation_Keys(md,qt[z].file1_ID,wl_ptr -> predicates[i].tuples[j].file2_column,interv_final -> final_rel,z);
//
//                         interv_final -> final_rel = Radix_Sort(interv_final -> final_rel);
//
//                         // check qt[rel], interv instead of this
//                         Join_v2(interv_final, interv_final -> final_rel, qt[rel_A_pos].rel, qt[z].file1_ID, wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID);
//
//                         break;
//
//                         // a bug exists here
//                     }
//
//                 }
//
//             }
//             else    // for the rest predicates
//             {
//                 uint64_t flag = 0;
//                 // search which id exists in qt
//                 for (size_t z = 0; z < rel_counter; z++)
//                 {
//                 //    printf("-j= %lu,z=%lu)%lu.%lu\n",j,  z, qt[z].file1_ID,qt[z].file1_column);
//                     // file1_ID.x = file1_ID
//                     if(wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID == qt[z].file1_ID)
//                     {
//
//                         for (size_t k = 0; k < interv_final -> position; k++)
//                     	{
//
//                     		if(interv_final -> rowId[k] == qt[z].file1_ID)
//                     		{
//
//                                 Update_Relation_Keys(md,qt[z].file1_ID,wl_ptr -> predicates[i].tuples[j].file1_column,interv_final ->final_rel,k);
//                                 // BUG HERE
//
//                             //    exit(-1);
//                                 interv_final ->final_rel = Radix_Sort(interv_final ->final_rel);
//                                 // Radix
//
//                                 rel_A_pos = Find_Query_Tuple(qt, wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file2_ID].file1_ID,wl_ptr -> predicates[i].tuples[j].file2_column,rel_counter);
//                             //    printf("j= %lu,z=%lu)%lu.%lu\n",j, rel_A_pos, qt[rel_A_pos].file1_ID,qt[rel_A_pos].file1_column);
//                                 if(rel_A_pos == 3)
//                                 {
//                                 //    Print_Relation_2(qt[rel_A_pos].rel);
//                                 }
//                             //    Print_Relation_2(interv_final -> final_rel);
//                             //    printf("%lu)%lu,%lu\n", z, qt[z].file1_ID, wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file2_ID].file1_ID);
//                                 Join_v2(interv_final, interv_final -> final_rel, qt[rel_A_pos].rel  , qt[z].file1_ID, wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file2_ID].file1_ID);
//                             //    Print_Relation_2(interv_final -> final_rel);
//                                 flag = 1;
//
//                                 break;
//                     		}
//                             //break;
//                     	}
//                         if(flag == 1)
//                         {
//                             flag = 0;
//                             break;
//                         }
//
//                     }
//                     if(wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file2_ID].file1_ID == qt[z].file1_ID)
//                     {
//
//                         for (size_t k = 0; k < interv_final -> position; k++)
//                         {
//                             if(interv_final -> rowId[k] == qt[z].file1_ID)
//                             {
//                                 uint64_t flag_1 = 0;
//                             //     Print_Relation_2(interv_final -> final_rel);
//                                 // exit(-1);
//                                 Update_Relation_Keys(md,qt[z].file1_ID,wl_ptr -> predicates[i].tuples[j].file2_column,interv_final ->final_rel,k);
//                                 interv_final ->final_rel = Radix_Sort(interv_final ->final_rel);
//                                 rel_A_pos = Find_Query_Tuple(qt, wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID,wl_ptr -> predicates[i].tuples[j].file1_column,rel_counter);
//                                 // Print_Relation_2(qt[rel_A_pos].rel);
//                                 // exit(-1);
//                             //    Print_Relation_2(interv_final -> final_rel);
//                                 // Prepsei na psa3w an uparxei to prwto kai oxi na to parw oloklhro
//
//                                 // 2nd repetition
//                                 for (size_t p = 0; p < interv_final -> position; p++)
//                                 {
//                                     // if both file_ID exists in intervening
//                                     if(interv_final -> rowId[p] == wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID)
//                                     {
//
//                                             relation * rel_temp;
//                                             rel_temp = Update_Relation(md, 0, 0, interv_final,1,1);
//                                             rel_temp = Radix_Sort(rel_temp);
//                                         //    Print_Relation_2(rel_temp);
//                                             //exit(-1);
//                                             flag_1 = 1;
//
//                                         //    printf("EDWWWWW\n");
//                                             Join_v2(interv_final, interv_final -> final_rel, rel_temp , qt[z].file1_ID, wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID);
//                                         //    Print_Relation_2(interv_final -> final_rel);
//
//                                         //    printf("2EDWWWWW, %lu , %lu\n", qt[z].file1_ID, wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID);
//                                             // for (size_t w = 0; w < interv_final -> position; w++)
//                                             // {
//                                             //     printf("-%lu \n",interv_final -> rowId[w]);
//                                             // }
//                                             // //
//                                             break;
//
//                                     }
//                                 }
//                                 if(flag_1 == 0)
//                                 {
//
//                                     Join_v2(interv_final, interv_final -> final_rel, qt[rel_A_pos].rel  , qt[z].file1_ID, wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID);
//
//                                 }
//
//                                 printf("\n");
//                             //     qt[2].rel = Update_Relation(md, 0, 0, interv_final,1,1);
//                             //     qt[2].rel = Radix_Sort(qt[2].rel);
//                             // //    Print_Relation_2(qt[2].rel);
//
//                                 flag = 1;
//
//                                 break;
//                     		}
//                     	}
//                         if(flag == 1)
//                         {
//                             flag = 0;
//                             break;
//                         }
//                     }
//                 }
//
//            }
//
//             printf("\n");
//
//         }
//
//
//         if(interv_final -> position == 0)
//         {
//             for (size_t z = 0; z < rel_counter; z++)
//             {
//                 printf("NULL ");
//             }
//             printf("\n");
//
//         }
//         else
//         {
//
//
//
//
//         printf("%lu)",i);
//         for (uint64_t z = 0; z < wl_ptr -> selects[i].num_tuples; z++) // for every select
//         {
//             for (uint64_t x = 0; x < interv_final -> position; x++)   // Search in interval final in rowid
//             {
//
//                 if(interv_final -> rowId[x] == wl_ptr -> parameters[i].tuples[wl_ptr -> selects[i].tuples[z].file1_ID].file1_ID)
//                 {
//
//                     CheckSum(md,wl_ptr -> parameters[i].tuples[wl_ptr -> selects[i].tuples[z].file1_ID].file1_ID,wl_ptr -> selects[i].tuples[z].file1_column,interv_final -> final_rel,x);
//
//                     break;
//                 }
//             }
//         }
//     }
//     //    exit(-1);
//
//         printf("\n");
//
//         rel_counter = 0;
//         free(qt);
//         free(interv_final -> final_rel);
//         free(interv_final);
//
//     }
//
//
//
// }
