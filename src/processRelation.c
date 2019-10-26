
#include "../include/processRelation.h"
#include "../include/quicksort.h"

#define L1_CACHE 65536    //  = 64 kb

void ProcessRelation(relation * rel_old, histogram * hist, psum * ps, relation * rel_new)
{
    uint64_t i = 0;
    uint64_t size = 0;
    printf("-------------------------------\n");
    while(i < hist -> num_tuples)
    {
        size = hist -> hist_tuples[i].sum * sizeof(tuple);

        printf("PR i = %ld - %ld\n",i,size);
        if(size < L1_CACHE)
        {
            if(i == hist -> num_tuples - 1)
            {
                Quicksort(rel_new, ps -> psum_tuples[i].position,ps -> psum_tuples[i].position + hist -> hist_tuples[i].sum - 1);
            }
            else
            {
                Quicksort(rel_new,ps -> psum_tuples[i].position,ps -> psum_tuples[i + 1].position -1);
            }
        }
        else
        {
            // split
            Clean_Relation(rel_old);
            histogram struct_h2;
            histogram *hist2 = &struct_h2;
            psum struct_p2;
            psum *ps2 = &struct_p2;
            Split_Bucket(rel_old,hist2,ps2,rel_new,ps->psum_tuples[i].position,ps->psum_tuples[i + 1].position,1);
            //Print_Relation(rel_new,hist2,ps2);
            ProcessRelation(rel_old,hist2,ps2,rel_new);
        }
        i++;
    }

}


void Print_Relation(relation * rel, histogram * hist, psum * ps)
{
    for (uint64_t i = 0; i < ps -> num_tuples; i++)
    {
        printf("------ Bucket : %ld -------\n",i);
        if(i == ps -> num_tuples - 1)
        {
            for(uint64_t j = ps -> psum_tuples[i].position; j < ps -> psum_tuples[i].position + hist -> hist_tuples[i].sum; j++)
            {
                printf("1key = %ld,  payload = %ld\n", rel->tuples[j].key, rel->tuples[j].payload);
            }
            printf("------------------------------\n");
            break;
        }
        for(uint64_t j = ps -> psum_tuples[i].position; j < ps -> psum_tuples[i + 1].position; j++)
        {
            printf("key = %ld,  payload = %ld\n", rel->tuples[j].key, rel->tuples[j].payload);
        }
        printf("------------------------------\n");

    }
}
