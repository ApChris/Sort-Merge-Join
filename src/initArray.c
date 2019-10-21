
#include "../include/initArray.h"

int64_t ** initArray(int64_t rows, int64_t columns)
{
    int64_t ** array;

    // Allocate memory for an array of pointers
    if((array = (int64_t **)malloc(rows * sizeof(int64_t *))) == NULL)
    {
        perror("initArray.c , first malloc\n");
        exit(-1);
    }

    // Allocate memory for every row
    for(int64_t i = 0; i < rows; i++)
    {
        if((array[i] = (int64_t *)malloc(columns * sizeof(int64_t))) == NULL)
        {
            perror("initArray.c , second malloc\n");
            exit(-1);
        }
    }

    // current time as seed
    srand(time(0));

    // Set a random number from 0 to 9
    for(int64_t i = 0; i < rows; i++)
    {
        for (int64_t j = 0; j < columns; j++) {
            array[i][j] = rand()%10;
        }
    }

    return array;
}
