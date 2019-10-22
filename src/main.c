#include <stdio.h>

#include "../include/getColumn.h"
#include "../include/initArray.h"

int main(int argc, char const *argv[])
{
    relation struct_R;
    relation *relation_R = &struct_R;

    int64_t **array_R;
    int64_t rows = 5;
    int64_t columns = 10;
    int64_t selected_column = 3;

    array_R = initArray(rows,columns);

    for(int i=0; i < rows; i++)
    {
        for (int64_t j = 0; j < columns; j++) {
            printf("%ld ", array_R[i][j]);
        }
        printf("\n");
    }

    getColumn(array_R,rows,selected_column,relation_R);

    for (int64_t i = 0; i < rows; i++) {
        printf("%ld, %ld\n", relation_R->tuples[i].key, relation_R->tuples[i].payload);
    }

    return 0;
}
