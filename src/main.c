#include <stdio.h>

#include "../include/getColumn.h"
#include "../include/initArray.h"
#include "../include/histogram.h"
#include "../include/psum.h"
int main(int argc, char const *argv[])
{
    relation struct_R;
    relation *relation_R = &struct_R;

    uint64_t **array_R;
    uint64_t rows = 5;
    uint64_t columns = 10;
    uint64_t selected_column = 3;

    array_R = initArray(rows,columns);

    for(int i=0; i < rows; i++)
    {
        for (uint64_t j = 0; j < columns; j++) {
            printf("%ld ", array_R[i][j]);
        }
        printf("\n");
    }

    getColumn(array_R,rows,selected_column,relation_R);

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
    Print_Psum(ps);
    return 0;
}
