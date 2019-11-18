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

    metadata * md;

    uint64_t * ptr;

    md = Read_Init_Binary("workloads/small/small.init");

    for (size_t i = 0; i < 14; i++)
    {
        printf("%lu) tuples = %lu\tcolumns = %lu\t----> ",i,  md[i].num_tuples, md[i].num_columns);

        for (size_t j = 0; j < md[i].num_columns; j++)
        {
            ptr = md[i].array[j];
            printf("%lu|",ptr[j]);
        }
        printf("\n");
    }

    return 0;
}
