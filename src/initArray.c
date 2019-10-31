
#include "../include/initArray.h"

uint64_t ** InitArray(uint64_t rows, uint64_t columns)
{
    uint64_t ** array;

    // Allocate memory for an array of pointers
    if((array = (uint64_t **)malloc(rows * sizeof(uint64_t *))) == NULL)
    {
        perror("initArray.c , first malloc\n");
        exit(-1);
    }

    // Allocate memory for every row
    for(uint64_t i = 0; i < rows; i++)
    {
        if((array[i] = (uint64_t *)malloc(columns * sizeof(uint64_t))) == NULL)
        {
            perror("initArray.c , second malloc\n");
            exit(-1);
        }
    }

    // current time as seed
    srand(time(0));

    // Set a random number from 0 to 999999
    for(uint64_t i = 0; i < rows; i++)
    {
        for (uint64_t j = 0; j < columns; j++) {
            array[i][j] = rand()%50000000;
        }
    }

    return array;
}
