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



  ProcessRelation(relation_A,histA,psA,relation_Anew,6);


   free(histA->hist_tuples);
   free(psA->psum_tuples);
   free(relation_A->tuples);


     relation struct_B;
     relation *relation_B = &struct_B;

     uint64_t **array_B;


GetColumn_FromFILE("Datasets/small/relB",relation_B);
uint64_t rowsB = relation_B -> num_tuples;


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


    free(histB->hist_tuples);
    free(psB->psum_tuples);
    free(relation_B->tuples);



     printf("\n\n------------------------------RESULTS------------------------------\n");



     result * head = NULL;
     head = resultInit();
     result * head2 = head;

     uint64_t size_A = 0;
     uint64_t size_B = 0;
     double time_spent;
     uint64_t counter = 0;
     clock_t begin;
     clock_t end;

      begin = clock();
     //counter += Join(relation_Anew, psA -> psum_tuples[i].position, psA -> psum_tuples[i + 1].position, relation_Bnew, psB -> psum_tuples[j].position, psB -> psum_tuples[j + 1].position,histA -> hist_tuples[i].splitsCounter, head);
     counter = Join(relation_Anew, relation_Bnew, head);
     end = clock();
     time_spent += (double)(end - begin) / CLOCKS_PER_SEC;

    //  for(uint64_t i = 0; i < histA -> num_tuples; i++)
    //  {
    //
    //      if(histA -> hist_tuples[i].sum == 0)
    //      {
    //          continue;
    //      }
    //      for(uint64_t j = 0; j < histB -> num_tuples; j++)
    //      {
    //          if(histB -> hist_tuples[j].sum == 0)
    //          {
    //              continue;
    //          }
    //
    //         if(i == j) // that bucket exists at both relations
    //         {
    //             clock_t begin = clock();
    //             //counter += Join(relation_Anew, psA -> psum_tuples[i].position, psA -> psum_tuples[i + 1].position, relation_Bnew, psB -> psum_tuples[j].position, psB -> psum_tuples[j + 1].position,histA -> hist_tuples[i].splitsCounter, head);
    //             counter += Join(relation_Anew, relation_Bnew, head);
    //             end = clock();
    //             time_spent += (double)(end - begin) / CLOCKS_PER_SEC;
    //         }
    //
    //     }
    // }


     printf("---------------------------------------------------------------------\n");
     printf("Elements in Result: %lu\n",counter);
     printf("time = %f s\n",time_spent);



     free(relation_Anew -> tuples);
     free(relation_Bnew -> tuples);

     Deallocate_List(head2);


    return 0;
}
