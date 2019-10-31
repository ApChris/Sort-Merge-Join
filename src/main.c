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

    // uint64_t **array_R;
    // uint64_t rows = 2500;
    // uint64_t columns = 10;
    // uint64_t selected_column = 3;
    //
    // array_R = InitArray(rows,columns);
    //
    // for(uint64_t i = 0; i < rows; i++)
    // {
    //     for (uint64_t j = 0; j < columns; j++) {
    //         printf("%ld ", array_R[i][j]);
    //     }
    //     printf("\n");
    // }
    //
    // GetColumn(array_R,rows,selected_column,relation_R);

    //
    //printf("%d\n",sizeof(relation_R->tuples) );
    /* for (uint64_t i = 0; i < relation_R -> num_tuples ; i++) {
         printf("%lu -> %lu, %lu\n",i ,relation_R->tuples[i].key, relation_R->tuples[i].payload);
     }
*/
    GetColumn_FromFILE("Datasets/medium/relA",relation_R);
    int64_t rows = relation_R -> num_tuples;

    histogram struct_h;
    histogram *hist = &struct_h;
    Histogram(relation_R,hist,0,0,rows);
    Print_Histogram(hist);


    psum struct_p;
    psum *ps = &struct_p;
    Psum(hist,ps,0);
    Print_Psum(hist,ps);

    // MEXRI EDW EXEI SWSTA APOTELESMATA
    relation struct_Rnew;
    relation *relation_Rnew = &struct_Rnew;
    ReorderedColumn(relation_R,relation_Rnew,ps);


    RestorePsum(hist, ps);  //psum's position returns to its initial values


    histogram struct_h2;
    histogram *hist2 = &struct_h2;
    psum struct_p2;
    psum *ps2 = &struct_p2;

    Print_Relation(relation_Rnew,hist,ps);
    //
    //
     ProcessRelation(relation_R,hist,ps,relation_Rnew,1);
    // Print_Relation(relation_Rnew,hist,ps);



    return 0;
}
