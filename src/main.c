#include <stdio.h>

#include "../include/getColumn.h"
#include "../include/initArray.h"
#include "../include/histogram.h"
#include "../include/psum.h"
#include "../include/reorderedColumn.h"
#include "../include/quicksort.h"

int main(int argc, char const *argv[])
{
    relation struct_R;
    relation *relation_R = &struct_R;

    uint64_t **array_R;
    uint64_t rows = 2500;
    uint64_t columns = 10;
    uint64_t selected_column = 3;

    array_R = InitArray(rows,columns);

    for(int i=0; i < rows; i++)
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
    Histogram(relation_R,hist);
    Print_Histogram(hist);

    psum struct_p;
    psum *ps = &struct_p;
    Psum(hist,ps);
    Print_Psum(hist,ps);


    relation struct_Rnew;
    relation *relation_Rnew = &struct_Rnew;
    ReorderedColumn(relation_R,relation_Rnew,ps);

    for (uint64_t i = 0; i < rows; i++) {
        printf("%ld, %ld\n", relation_Rnew->tuples[i].key, relation_Rnew->tuples[i].payload);
    }


    RestorePsum(hist, ps);  //psum's position returns to its initial values

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
