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





metadata * Read_Init_Binary(const char * filename)
{

    // variables for init file
    FILE * file_init;
    uint64_t rows = 0;
    char current_path[4];
    char ch;


    // variables for binary files
    FILE * file_binary;
    uint64_t length_binary = 0;
    uint64_t * array;


    // variables for metadata struct
    metadata * md;
    uint64_t offset_metadata = 2;


    if((file_init = fopen(filename,"rb+")) == NULL)
    {
        perror("file_init fopen failed:");
        exit(-1);
    }


    // Get the rows of init file
    ch = getc(file_init);
    for(ch = getc(file_init); ch != EOF; ch = getc(file_init))
    {
        if(ch == '\n')
        {
            rows++;
        }
    }
    rewind(file_init);


    if((md = (uint64_t *)malloc(rows*sizeof(metadata))) == NULL)
    {
        perror("Read_binary_file malloc failed:");
        exit(-1);
    }

    // for every binary file
    for (size_t i = 0; i < rows; i++)
    {
        fscanf(file_init,"%s\n",current_path);
        //printf("%s\n",current_path);
        char final_path[22] = "workloads/small/";
        strcat(final_path,current_path);

        if((file_binary = fopen(final_path,"rb+")) == NULL)
        {
            perror("fopen failed:");
            exit(-1);
        }

        // length == all bytes in file but we divide with 8 , because we use uint64_t
        fseek(file_binary, 0, SEEK_END);
        length_binary = ftell(file_binary);
        fseek(file_binary, 0, SEEK_SET);


        // Allocate memory
        if((array = (uint64_t *)malloc((length_binary/8 + 1)*sizeof(uint64_t))) == NULL)
        {
            perror("File:malloc failed:");
            exit(-1);
        }
        for (size_t i = 0; i < length_binary/8; i++)
        {
            array[i] = 0;
        }

        // start reading
        fread(array,length_binary,1,file_binary);

        md[i].num_tuples = array[0];
        md[i].num_columns = array[1];
        if((md[i].array = (uint64_t *)malloc(array[1]*sizeof(uint64_t *))) == NULL)
        {
            perror("File:malloc2 failed:");
            exit(-1);
        }
        for (size_t j = 0; j < array[1]; j++)
        {
        //    printf("value = %lu --> address = %p\n",array[offset_metadata],&array[offset_metadata]);
            md[i].array[j] = &array[offset_metadata];
            offset_metadata += array[0];

        }

        offset_metadata = 2;
        // print
        // for (size_t i = 2; i < array[0] + 2; i++)
        // {
        //     for (size_t j = 0; j < array[1]; j++)
        //     {
        //         printf("%lu|",array[i+offset]);
        //         offset += array[0];
        //     }
        //     offset = 0;
        //     printf("\n");
        // }


        // close
        if(fclose(file_binary))
        {
            perror("fclose failed:");
            exit(-1);
        }

    }
    // close
    if(fclose(file_init))
    {
        perror("finit failed:");
        exit(-1);
    }
    return md;
}



void Read_Work(const char * filename)
{

    // variables for init file
    FILE * file;
    uint64_t length = 0;
    uint64_t read;
    char seps[] = "\n,()&|_ ";
    char * line = NULL;

    // open work file
    if((file = fopen(filename,"rb+")) == NULL)
    {
        perror("file_init fopen failed:");
        exit(-1);
    }

    while ((read = getline(&line, &length, file)) != -1)
    {
        char * token;
        printf("%s\n",line);

        uint64_t * parameters = (uint64_t*)malloc(sizeof(uint64_t));
        int num_parameters = 0;

        char ** predicates = (char**)malloc(sizeof(char*));
        int num_predicates = 0;

        char ** filters = (char**)malloc(sizeof(char*));
        int num_filters = 0;

        char ** selects = (char**)malloc(sizeof(char*));
        int num_selects = 0;



        token = strtok(line,seps);
        

        while(token != NULL)
        {
            // printf("token: %s\n", token);
            if ((strchr(token, '=') == NULL) && (strchr(token, '.') == NULL))
            {
                parameters[num_parameters] = atoi(token);
                num_parameters++;
                token = strtok(NULL,seps);
                if((parameters = (uint64_t*)realloc(parameters, sizeof(uint64_t)*(num_parameters+1)))==NULL){
                    perror("realloc, Read_Work parameters");
                    exit(-1);
                }
                continue;
            }
            if ((strchr(token, '=') != NULL) && (strchr(token, '.') != NULL))
            {
                predicates[num_predicates] = (char*)malloc((strlen(token)+1)*sizeof(char*));
                strcpy(predicates[num_predicates], token);
                num_predicates++; 
                token = strtok(NULL,seps);
                if((predicates = (char**)realloc(predicates, sizeof(char*)*(num_predicates+1))) == NULL){
                    perror("realloc, Read_Work predicates");
                    exit(-1);
                }
                continue;
            }
            if ((strchr(token, '>') != NULL) || (strchr(token, '<') != NULL))
            { 
                filters[num_filters] = (char*)malloc((strlen(token)+1)*sizeof(char*));
                strcpy(filters[num_filters], token);
                num_filters++;
                token = strtok(NULL,seps);
                if ((filters = (char*)realloc(filters, sizeof(char*)*(num_filters+1))) == NULL)
                {
                    perror("realloc, Read_Work filters");
                    exit(-1);
                }
                continue;
            }
            if ((strchr(token, '=') == NULL) && (strchr(token, '.') != NULL) && (strchr(token, '>') == NULL) || (strchr(token, '<') == NULL))
            {
                selects[num_selects] = (char*)malloc((strlen(token)+1)*sizeof(char*));
                strcpy(selects[num_selects], token);
                num_selects++;
                token = strtok(NULL,seps);
                if ((selects = (char*)realloc(selects, sizeof(char*)*(num_selects+1))) == NULL)
                {
                    perror("realloc, Read_Work selects");
                    exit(-1);
                }
                continue;
            }
        }

        for (int i = 0; i < num_parameters; i++)
        {
            printf("parameters[%d]: %lu\n", i, parameters[i]);
        }
        for (int i = 0; i < num_predicates; i++)
        {
            printf("predicates[%d]: %s\n", i, predicates[i]);
        }
        for (int i = 0; i < num_filters; i++)
        {
            printf("filters[%d]: %s\n", i, filters[i]);
        }
        for (int i = 0; i < num_selects; i++)
        {
            printf("selects[%d]: %s\n", i, selects[i]);
        }
        
        printf("\n");
        // if(strcmp(token,"F"))
        // {
        //     break;
        // }
        // //break;


        free(parameters);

        for (int i = 0; i < num_predicates; i++)
        {
            free(predicates[i]);
        }
        free(predicates);

        for (int i = 0; i < num_filters; i++)
        {
            free(filters[i]);
        }
        free(filters);

        for (int i = 0; i < num_selects; i++)
        {
            free(selects[i]);
        }
        free(selects);

    }

    //ch = strtok( , "|");

    // length == all bytes in file but we divide with 8 , because we use uint64_t
    // fseek(file, 0, SEEK_END);
    // length = ftell(file);
    // fseek(file, 0, SEEK_SET);

    if(line)
    {
        free(line);
    }

    printf("\n\n\n");
    // close
    if(fclose(file))
    {
        perror("fclose failed:");
        exit(-1);
    }



}
