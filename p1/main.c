
#include <stdio.h>
#include <time.h>
#include <string.h>
#include "../include/getColumn.h"
#include "../include/initArray.h"
#include "../include/reorderedColumn.h"
#include "../include/quicksort.h"
#include "../include/splitBucket.h"
#include "../include/result.h"
#include "../include/sortMergeJoin.h"
#include "../include/processRelation.h"

int main(int argc, char const *argv[])
{


relation struct_A;
relation * relation_A = &struct_A;

relation struct_A_final;
relation * relation_A_final = &struct_A_final;

relation struct_B;
relation * relation_B = &struct_B;

relation struct_B_final;
relation * relation_B_final = &struct_B_final;

result struct_r;
result * res = &struct_r;

if(argc == 2)
{
    if(!strcmp(argv[1],"tiny"))
    {
        GetColumn_FromFILE("Datasets/tiny/relA",relation_A);
        GetColumn_FromFILE("Datasets/tiny/relB",relation_B);
    }
    else if(!strcmp(argv[1],"small"))
    {
        GetColumn_FromFILE("Datasets/small/relA",relation_A);
        GetColumn_FromFILE("Datasets/small/relB",relation_B);
    }
    else if(!strcmp(argv[1],"medium"))
    {
        GetColumn_FromFILE("Datasets/medium/relA",relation_A);
        GetColumn_FromFILE("Datasets/medium/relB",relation_B);
    }
    else
    {
        printf("Wrong argument! Try again\n");
        exit(-1);
    }
}
else if(argc > 2)
{
    printf("\nToo many arguments! Try again\n\n");
    exit(-1);
}
else
{
    printf("\nYou have to run one of the following:\n./smj tiny\n./smj small\n./smj medium\n\nTry again!!\n\n");
    exit(-1);
}

Final_Relation(relation_A, relation_A_final);
Final_Relation(relation_B, relation_B_final);

res = SortMergeJoin(relation_A_final, relation_B_final);

//printResult(res);
Deallocate_List(res);

return 0;

}
