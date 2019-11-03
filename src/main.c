#include <stdio.h>
#include <time.h>
#include "../include/getColumn.h"
#include "../include/initArray.h"
#include "../include/reorderedColumn.h"
#include "../include/quicksort.h"
#include "../include/splitBucket.h"
#include "../include/result.h"

int main(int argc, char const *argv[])
{


    relation struct_A;
    relation *relation_A = &struct_A;

    GetColumn_FromFILE("Datasets/small/relA",relation_A);
    uint64_t rowsA = relation_A -> num_tuples;

    histogram struct_hA;
    histogram *histA = &struct_hA;
    Histogram(relation_A,histA,7,0,rowsA);
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



    //Print_Relation(relation_A,histA,psA);

    //

  ProcessRelation(relation_A,histA,psA,relation_Anew,6);

  // for(uint64_t i = 0; i < relation_Anew -> num_tuples; i++)
  //     {
  //         printf("%lu)key = %lu, payload = %lu\n",i,relation_Anew -> tuples[i].key,relation_Anew -> tuples[i].payload);
  //     }
//
// //    Print_Relation(relation_Anew,histA,psA);
//      //Print_Relation(relation_Rnew,hist,ps);
//
//
//
     relation struct_B;
     relation *relation_B = &struct_B;

     uint64_t **array_B;


GetColumn_FromFILE("Datasets/small/relB",relation_B);
uint64_t rowsB = relation_B -> num_tuples;

// for(uint64_t i = 0; i < relation_B -> num_tuples; i++)
// {
//     printf("%lu,%lu\n",relation_B -> tuples[i].key,relation_B -> tuples[i].payload);
// }
     histogram struct_hB;
     histogram *histB = &struct_hB;
     Histogram(relation_B,histB,7,0,rowsB);
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

    ProcessRelation(relation_B,histB,psB,relation_Bnew,6);




    // Print_Histogram(histA);
    // Print_Psum(histA,psA);
     // Print_Relation(relation_Anew,histA,psA);
    // Print_Relation(relation_Bnew,histB,psB);




    //
     printf("\n\n------------------------------RESULTS------------------------------\n");


    // Print_Relation(relation_Anew,histA,psA);
     //Print_Relation(relation_Bnew,histB,psB);
     //


//     printResult(head);

     // for(uint64_t i = 0; i < relation_Bnew->num_tuples; i++)
     // {
     //     printf("%lu) key = %lu, payload = %lu\n",i,relation_Bnew->tuples[i].key,relation_Bnew->tuples[i].payload);
     // }
     // uint64_t size_A = 0;
     // uint64_t size_B = 0;
     // double time_spent;
     // uint64_t counter;
     // for(uint64_t i = 0; i < histA -> num_tuples; i++)
     // {

     //     if(histA -> hist_tuples[i].sum == 0)
     //     {
     //         continue;
     //     }
     //     for(uint64_t j = 0; j < histB -> num_tuples; j++)
     //     {
     //         if(histB -> hist_tuples[j].sum == 0)
     //         {
     //             continue;
     //         }

     //         if(i == j) // that bucket exists at both relations
     //         {

     //            printf("Bucket: %lu <----exists at both relations \n",i);
     //            clock_t begin = clock();

     //            //Join(relation_Anew, relation_Bnew,1, head);
     //            // if(histA -> hist_tuples[i].splitsCounter == 0 && histA -> hist_tuples[i].splitsCounter == 0)
     //            // {
     //            //     counter = Join(relation_Anew, psA -> psum_tuples[i].position, psA -> psum_tuples[i + 1].position, relation_Bnew, psB -> psum_tuples[j].position, psB -> psum_tuples[j + 1].position,histA -> hist_tuples[i].splitsCounter, head);
     //            //
     //            // }
     //            // else
     //            // {
     //                counter += Join(relation_Anew, psA -> psum_tuples[i].position, psA -> psum_tuples[i + 1].position, relation_Bnew, psB -> psum_tuples[j].position, psB -> psum_tuples[j + 1].position,histA -> hist_tuples[i].splitsCounter, head);
     //            //}
     //            clock_t end = clock();

     //            time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
     //         }

     //     }
     // }
    
    result * head = NULL;
    head = resultInit();
    result * head2 = head;

    double time_spent;
    clock_t begin = clock();
    uint64_t counter = Join(relation_Anew, relation_Bnew, head);
    clock_t end = clock();
    time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
    printResult(head2);

// Print_Histogram(histB);
 //Print_Relation(relation_Anew,histA,psA);
//  Print_Relation(relation_Bnew,histB,psB);
     printf("---------------------------------------------------------------------\n");
     printf("Elements in Result: %lu\n",counter);
     printf("time = %lf s\n",time_spent);
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
