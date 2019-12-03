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

char * split(char * str, const char * delim)
{
    char * p = strstr(str, delim);

    if (p == NULL)
        return NULL;

    *p = '\0';
    return p + strlen(delim);
}

uint64_t self_join_check(char * str1, char * str2)
{
    char * tail_str1, * tail_str2;

    tail_str1 = split(str1, ".");
    tail_str2 = split(str2, ".");

    if (!strcmp(str1, str2))
    {
        return 0;
    }
    return -1;
}

void Read_Work(metadata * md, const char * filename)
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


    uint64_t parameter_flag = 0;
    uint64_t predicate_flag = 0;
    uint64_t filter_flag = 0;
    uint64_t select_flag = 0;

    work_line * wl_ptr = WorkLineInit();

    while ((read = getline(&line, &length, file)) != -1)
    {

        char * token;
        printf("\nNew query:\t");
        printf("%s",line);

        uint64_t * parameters = (uint64_t*)malloc(sizeof(uint64_t));
        uint64_t num_parameters = 0;

        char ** predicates = (char**)malloc(sizeof(char*));
        uint64_t num_predicates = 0;

        char ** filters = (char**)malloc(sizeof(char*));
        uint64_t num_filters = 0;

        char ** selects = (char**)malloc(sizeof(char*));
        uint64_t num_selects = 0;

        uint64_t F_flag = 0;


        token = strtok(line,seps);

        while(token != NULL)
        {
            // printf("token: %s\n", token);
            if(strchr(token, 'F'))
            {
                F_flag = 1;
                break;
            }
            // PARAMETERS
            if ((strchr(token, '=') == NULL) && (strchr(token, '.') == NULL))
            {
                printf("EDWWW = %s\n",token);
                parameters[num_parameters] = atoi(token);
                num_parameters++;
                token = strtok(NULL,seps);
                if((parameters = (uint64_t*)realloc(parameters, sizeof(uint64_t)*(num_parameters+1)))==NULL){
                    perror("realloc, Read_Work parameters");
                    exit(-1);
                }
                printf("EDWWW = %s\n",token);

                continue;
            }

            // PREDICATES
            if ((strchr(token, '=') != NULL) && (strchr(token, '.') != NULL))
            {
                char * predicate = (char*)malloc(strlen(token)+1);
                char * predicate_tail;

                strcpy(predicate, token);  //  temp_token has the string of the token
                predicate_tail = split(predicate, "=");   // predicate_tail: string after delimeter
                                                             // and predicate: string before delimeter
                char * file1_ID = (char*)malloc(strlen(predicate)+1);
                strcpy(file1_ID, predicate);
                char * file1_Column = split(file1_ID, ".");

                char * file2_ID = (char*)malloc(strlen(predicate_tail)+1);
                strcpy(file2_ID, predicate_tail);
                char * file2_Column = split(file2_ID, ".");



                char * temp_predicate_tail = (char*)malloc(strlen(predicate_tail)+1);
                strcpy(temp_predicate_tail, predicate_tail);


                // self join, i.e. 0.1=0.2 (scanning)
                if (!self_join_check(predicate, temp_predicate_tail))
                {
                    printf("self join\n");
                }



                // i.e. 0.1=5000 is a filter, not a predicate
                if ((strchr(predicate_tail, '.') == NULL))
                {
                    filters[num_filters] = (char*)malloc((strlen(token)+1)*sizeof(char*));
                    strcpy(filters[num_filters], token);
                    num_filters++;
                    if ((filters = (char*)realloc(filters, sizeof(char*)*(num_filters+1))) == NULL)
                    {
                        perror("realloc, Read_Work filters");
                        exit(-1);
                    }
                }
                // regular predicate, i.e. 0.1=1.2
                else
                {
                    predicates[num_predicates] = (char*)malloc((strlen(token)+1)*sizeof(char*));
                    strcpy(predicates[num_predicates], token);
                    num_predicates++;
                    if((predicates = (char**)realloc(predicates, sizeof(char*)*(num_predicates+1))) == NULL){
                        perror("realloc, Read_Work predicates");
                        exit(-1);
                    }

                    if(predicate_flag == 0)
                    {
                        PredicateRelInit(wl_ptr);
                        Push_Predicates(wl_ptr,atoi(file1_ID),atoi(file1_Column),atoi(file2_ID),atoi(file2_Column),1);
                        predicate_flag = 1;
                    }
                    else
                    {
                        Push_Predicates(wl_ptr,atoi(file1_ID),atoi(file1_Column),atoi(file2_ID),atoi(file2_Column),0);
                    }
                }
                free(predicate);
                free(temp_predicate_tail);
                free(file1_ID);
                free(file2_ID);
                token = strtok(NULL,seps);
                continue;
            }

            // FILTERS
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

            // SELECTS
            if ((strchr(token, '=') == NULL) && (strchr(token, '.') != NULL) && (strchr(token, '>') == NULL) && (strchr(token, '<') == NULL))
            {
                char * select = (char*)malloc(strlen(token)+1);
                char * select_tail;

                strcpy(select, token);  //  temp_token has the string of the token
                select_tail = split(select, ".");   // select_tail: string after delimeter
                                                             // and select: string before delimeter


                printf("%s ",select);
                printf("%s\n",select_tail);


                selects[num_selects] = (char*)malloc((strlen(token)+1)*sizeof(char*));
                strcpy(selects[num_selects], token);
                num_selects++;
                token = strtok(NULL,seps);
                if ((selects = (char*)realloc(selects, sizeof(char*)*(num_selects+1))) == NULL)
                {
                    perror("realloc, Read_Work selects");
                    exit(-1);
                }

                if(select_flag == 0)
                {
                    SelectsRelInit(wl_ptr);
                    Push_Selects(wl_ptr,atoi(select),atoi(select_tail),1);
                    select_flag = 1;
                }
                else
                {
                    Push_Selects(wl_ptr,atoi(select),atoi(select_tail),0);
                }
                free(select);

                continue;
            }
        }

        if (F_flag == 1)
        {
            break;
        }
        predicate_flag = 0;
        select_flag = 0;
        parameter_flag = 0;
        //wl_ptr -> num_predicates++;
        //Process_Function(md, parameters, predicates, filters, selects, num_parameters, num_predicates, num_filters, num_selects);

        // for (uint64_t i = 0; i < num_parameters; i++)
        // {
        //     printf("parameters[%lu]: %lu\n", i, parameters[i]);
        // }
        // for (uint64_t i = 0; i < num_predicates; i++)
        // {
        //     printf("predicates[%lu]: %s\n", i, predicates[i]);
        // }
        // for (uint64_t i = 0; i < num_filters; i++)
        // {
        //     printf("filters[%lu]: %s\n", i, filters[i]);
        // }
        // for (uint64_t i = 0; i < num_selects; i++)
        // {
        //     printf("selects[%lu]: %s\n", i, selects[i]);
        // }



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

    // printf("\n\n\n");
    // close
    if(fclose(file))
    {
        perror("fclose failed:");
        exit(-1);
    }
    printf("Queries number: %lu\n", wl_ptr -> num_predicates);

    for (size_t i = 0; i < wl_ptr -> num_predicates; i++)
    {
        printf("->%lu\n",wl_ptr -> predicates[i].num_tuples);
        for (size_t j = 0; j < wl_ptr -> predicates[i].num_tuples; j++)
        {
            printf("%lu.",wl_ptr -> predicates[i].tuples[j].file1_ID);
            printf("%lu=",wl_ptr -> predicates[i].tuples[j].file1_column);
            printf("%lu.",wl_ptr -> predicates[i].tuples[j].file2_ID);
            printf("%lu& ",wl_ptr -> predicates[i].tuples[j].file2_column);
        }
        printf("\n");

    }

}
