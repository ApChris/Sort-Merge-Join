#include <stdio.h>

#include "../include/getColumn.h"
#include "../include/initArray.h"
#include "../include/reorderedColumn.h"
#include "../include/quicksort.h"
#include "../include/splitBucket.h"
#include "../include/result.h"

int main(int argc, char const *argv[])
{
/*
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
*/

    relation struct_A;
    relation *relation_A = &struct_A;

    uint64_t **array_A;
/*    uint64_t rowsA = 2500;
    uint64_t columnsA = 10;
    uint64_t selected_columnA = 3;

    array_A = InitArray(rowsA,columnsA);

    for(uint64_t i = 0; i < rowsA; i++)
    {
        for (uint64_t j = 0; j < columnsA; j++) {
            printf("%ld ", array_A[i][j]);
        }
        printf("\n");
    }

    GetColumn(array_A,rowsA,selected_columnA,relation_A);
*/
    GetColumn_FromFILE("Datasets/tiny/relA",relation_A);
    uint64_t rowsA = relation_A -> num_tuples;

    histogram struct_hA;
    histogram *histA = &struct_hA;
    Histogram(relation_A,histA,0,0,rowsA);
    Print_Histogram(histA);


    psum struct_pA;
    psum *psA = &struct_pA;
    Psum(histA,psA,0);
    Print_Psum(histA,psA);

    // MEXRI EDW EXEI SWSTA APOTELESMATA
    relation struct_Anew;
    relation *relation_Anew = &struct_Anew;
    ReorderedColumn(relation_A,relation_Anew,psA);


    RestorePsum(histA, psA);  //psum's position returns to its initial values


    //Print_Relation(relation_Anew,histA,psA);

    ProcessRelation(relation_A,histA,psA,relation_Anew,1);

     //Print_Relation(relation_Rnew,hist,ps);



     relation struct_B;
     relation *relation_B = &struct_B;

     uint64_t **array_B;
/*     uint64_t rowsB = 500;
     uint64_t columnsB = 5;
     uint64_t selected_columnB = 3;

     array_B = InitArray(rowsB,columnsB);

     for(uint64_t i = 0; i < rowsB; i++)
     {
         for (uint64_t j = 0; j < columnsB; j++) {
             printf("%ld ", array_B[i][j]);
         }
         printf("\n");
     }

     GetColumn(array_B,rowsB,selected_columnB,relation_B);
*/
GetColumn_FromFILE("Datasets/tiny/relB",relation_B);
uint64_t rowsB = relation_B -> num_tuples;

     histogram struct_hB;
     histogram *histB = &struct_hB;
     Histogram(relation_B,histB,0,0,rowsB);
     Print_Histogram(histB);


     psum struct_pB;
     psum *psB = &struct_pB;
     Psum(histB,psB,0);
     Print_Psum(histB,psB);

     // MEXRI EDW EXEI SWSTA APOTELESMATA
     relation struct_Bnew;
     relation *relation_Bnew = &struct_Bnew;
     ReorderedColumn(relation_B,relation_Bnew,psB);


     RestorePsum(histB, psB);  //psum's position returns to its initial values

     ProcessRelation(relation_B,histB,psB,relation_Bnew,1);

    // Print_Relation(relation_Anew,histA,psA);
     //Print_Relation(relation_Bnew,histB,psB);




    //
     printf("\n\n------------------------------RESULTS------------------------------\n");


     result * head = NULL;
     head = resultInit();

     printResult(head);
     printf("%lu\n",relation_Anew->num_tuples);
     printf("%lu\n",relation_Bnew->num_tuples);

     // for(uint64_t i = 0; i < relation_Bnew->num_tuples; i++)
     // {
     //     printf("%lu) key = %lu, payload = %lu\n",i,relation_Bnew->tuples[i].key,relation_Bnew->tuples[i].payload);
     // }
     uint64_t size_A = 0;
     uint64_t size_B = 0;
     for(uint64_t i = 0; i < histA -> num_tuples; i++)
     {

         if(histA -> hist_tuples[i].sum == 0)
         {
             continue;
         }
         for(uint64_t j = 0; j < histB -> num_tuples; j++)
         {
             if(histB -> hist_tuples[j].sum == 0)
             {
                 continue;
             }

             if(i == j) // that bucket exists at both relations
             {

                printf("%lu idia\n",i);
                Join(relation_Anew, psA -> psum_tuples[i].position, psA -> psum_tuples[i + 1].position, relation_Bnew, psB -> psum_tuples[j].position, psB -> psum_tuples[j + 1].position,histA -> hist_tuples[i].splitsCounter, head);
             }

         }
     }

//Print_Histogram(histB);
     printf("---------------------------------------------------------------------\n");
    return 0;
}



/*
GetColumn_FromFILE("Datasets/tiny/relB",relation_R);
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

//Print_Relation(relation_Rnew,hist,ps);
//
//
 ProcessRelation(relation_R,hist,ps,relation_Rnew,1);
 Print_Relation(relation_Rnew,hist,ps);



*/
