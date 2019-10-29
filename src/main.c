#include <stdio.h>

#include "../include/getColumn.h"
#include "../include/initArray.h"
#include "../include/reorderedColumn.h"
#include "../include/quicksort.h"
#include "../include/splitBucket.h"

int main(int argc, char const *argv[])
{
    relation struct_R;
    relation *relation_R = &struct_R;

    uint64_t **array_R;
    uint64_t rows = 2500;
    uint64_t columns = 10;
    uint64_t selected_column = 3;

    array_R = InitArray(rows,columns);

    for(uint64_t i = 0; i < rows; i++)
    {
        for (uint64_t j = 0; j < columns; j++) {
            printf("%ld ", array_R[i][j]);
        }
        printf("\n");
    }

    GetColumn(array_R,rows,selected_column,relation_R);

    for (uint64_t i = 0; i < rows; i++) {
        printf("%ld, %ld\n", relation_R->tuples[i].key, relation_R->tuples[i].payload);
    }



    histogram struct_h;
    histogram *hist = &struct_h;
    Histogram(relation_R,hist,0,0,rows);
    Print_Histogram(hist);


    psum struct_p;
    psum *ps = &struct_p;
    Psum(hist,ps,0);
    Print_Psum(hist,ps);
    //
    // MEXRI EDW EXEI SWSTA APOTELESMATA
    relation struct_Rnew;
    relation *relation_Rnew = &struct_Rnew;
    ReorderedColumn(relation_R,relation_Rnew,ps);

    /*for (uint64_t i = 0; i < rows; i++) {
        printf("%ld, %ld\n", relation_Rnew->tuples[i].key, relation_Rnew->tuples[i].payload);
    }
*/

    RestorePsum(hist, ps);  //psum's position returns to its initial values



    histogram struct_h2;
    histogram *hist2 = &struct_h2;
    psum struct_p2;
    psum *ps2 = &struct_p2;
    // Split_Bucket(relation_R,hist2,ps2,relation_Rnew,ps->psum_tuples[0].position,ps->psum_tuples[1].position,1);
    // Split_Bucket(relation_R,hist2,ps2,relation_Rnew,ps->psum_tuples[1].position,ps->psum_tuples[2].position,1);
    // Split_Bucket(relation_R,hist2,ps2,relation_Rnew,ps->psum_tuples[2].position,ps->psum_tuples[3].position,1);
    // Split_Bucket(relation_R,hist2,ps2,relation_Rnew,ps->psum_tuples[3].position,ps->psum_tuples[4].position,1);
    Print_Relation(relation_Rnew,hist,ps);
    printf("asdasdasdasdas-------------------------d\n");
    /*Split_Bucket(relation_R,hist,ps,relation_Rnew,ps->psum_tuples[0].position,ps->psum_tuples[1].position,1);
    Split_Bucket(relation_R,hist,ps,relation_Rnew,ps->psum_tuples[1].position,ps->psum_tuples[2].position,1);
    Split_Bucket(relation_R,hist,ps,relation_Rnew,ps->psum_tuples[2].position,ps->psum_tuples[3].position,1);*/
    //Print_Relation(relation_Rnew,hist,ps);
    for(int64_t i = 0; i < hist -> num_tuples; i++)
    {
        if( i == hist -> num_tuples - 1)
        {
            Split_Bucket(relation_R,hist,ps,relation_Rnew,ps->psum_tuples[i].position,ps->psum_tuples[i].position + hist -> hist_tuples[i].sum,1);
        }
        else
        {
            Split_Bucket(relation_R,hist,ps,relation_Rnew,ps->psum_tuples[i].position,ps->psum_tuples[i + 1].position,1);

            //printBucket();
        }
    }
    Print_Relation(relation_Rnew,hist,ps);
    // for(int64_t i = 0; i < hist2 -> num_tuples; i++)
    // {
    //     uint64_t size = 0;
    //     size = hist2 -> hist_tuples[i].sum * sizeof(tuple);
    //
    //     //printf("PR i = %ld - %ld\n",i,size);
    //     if(size < 8)
    //     {
    //
    //     }
    //     else
    //     {
    //         printf("SPLITTTTTT\n");
    //         histogram struct_h3;
    //         histogram *hist3 = &struct_h3;
    //         psum struct_p3;
    //         psum *ps3 = &struct_p3;
    //         if(i == hist -> num_tuples - 1)
    //         {
    //             Split_Bucket(relation_R,hist3,ps3,relation_Rnew,ps2->psum_tuples[i].position,ps2->psum_tuples[i].position + ps2 -> psum_tuples[i].position + hist2 -> hist_tuples[i].sum,2);
    //             printBucket(relation_Rnew,ps2->psum_tuples[i].position,ps2->psum_tuples[i].position + ps2 -> psum_tuples[i].position + hist2 -> hist_tuples[i].sum);
    //
    //
    //         }
    //         else
    //         {
    //             Split_Bucket(relation_R,hist3,ps3,relation_Rnew,ps2->psum_tuples[i].position,ps2->psum_tuples[i + 1].position,2);
    //             printBucket(relation_Rnew,ps2->psum_tuples[i].position,ps2->psum_tuples[i + 1].position);
    //         }
    //         break;
    //
    //     }
    // }


    // for (uint64_t i = 0; i < rows; i++) {
    //     printf("%ld, %ld\n", relation_Rnew->tuples[i].key, relation_Rnew->tuples[i].payload);
    // }

    //
    //ProcessRelation(relation_R,hist,ps,relation_Rnew);
    //Print_Relation(relation_Rnew,hist,ps);






    // uint64_t start=ps->psum_tuples[0].position;
    // uint64_t end=ps->psum_tuples[1].position;
    // printf("start: %ld, end: %ld\n", start, end);

    // printf("Initial bucket:\n");

    // printBucket(relation_Rnew, start, end);

    // printf("Quicksorted bucket:\n");

    // Quicksort(relation_Rnew, start, end-1);
    // printBucket(relation_Rnew, start, end);


    return 0;
}
