#include <stdio.h>
#include <stdlib.h>
#include "../include/getColumn.h"

void GetColumn(uint64_t ** array, uint64_t rows, uint64_t selected_column, relation *rel)
{

    // We'll create space for rows number of tuples
    if((rel -> tuples = (tuple *)malloc(rows * sizeof(tuple))) == NULL)
    {
        perror("getQolumn.c , first malloc\n");
        exit(-1);
    }

    uint64_t i = 0;

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


void GetColumn_FromFILE(const char * filename, relation *rel)
{
    FILE *file;
    uint64_t rows = 0;
    char ch;

    if((file = fopen(filename,"r")) == NULL)
    {
        perror("GetColumn_FromFILE.c error:");
        exit(-1);
    }

    ch = getc(file);
    for(ch = getc(file); ch != EOF; ch = getc(file))
    {
        if(ch == '\n')
        {
            rows++;
        }
    }
    rewind(file);

    // We'll create space for rows number of tuples
    if((rel -> tuples = (tuple *)malloc(rows * sizeof(tuple))) == NULL)
    {
        perror("GetColumn_FromFILE.c , first malloc\n");
        exit(-1);
    }

    uint64_t key = 0;
    uint64_t payload = 0;
    uint64_t i = 0;


    for(; i < rows; i++)
    {
        fscanf(file, "%lu,%lu",&key,&payload);
        rel -> tuples[i].key = key; // key is the value
        rel -> tuples[i].payload = payload; // payload

    }

    // The number of tuples is the number of rows
    rel -> num_tuples = rows;


    if(fclose(file) != 0)
    {
        perror("GetColumn_FromFILE.c error:");
        exit(-1);
    }

}
