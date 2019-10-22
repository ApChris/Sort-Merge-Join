
#include "../include/getColumn.h"

void getColumn(int64_t ** array, int64_t rows, int64_t selected_column, relation *rel)
{

    // We'll create space for rows number of tuples
    if((rel -> tuples = (tuple *)malloc(rows * sizeof(tuple))) == NULL)
    {
        perror("getQolumn.c , first malloc");
        exit(-1);
    }

    int64_t i = 0;

    // For each row
    while(i < rows)
    {
        // key is the value
        rel -> tuples[i].key = array[i][selected_column];

        // payload is the rowID
        rel -> tuples[i].payload = i;
        i++;
    }

    // The number of tuples is the number of rows
    rel -> num_tuples = rows;

}
