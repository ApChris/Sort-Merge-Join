#include "../include/execute_query.h"


uint64_t CheckSum(metadata * md, uint64_t md_row, uint64_t md_column, relation * rel, uint64_t pos)
{
    uint64_t * ptr = md[md_row].array[md_column];
    uint64_t sum = 0;
    for (uint64_t i = 0; i < rel -> num_tuples; i++)
    {
        sum += *(ptr + rel -> tuples[i].payload[pos]); // calculate keys
        //sum += rel -> tuples[i].payload[pos]; // calculate payloads
    }
    if(sum == 0)
    {
        printf("NULL ");
    }
    else
    {
        printf("%lu ",sum);
    }
    return sum;
}


relation * Init_pointer()
{
    relation * rel;
    if((rel = (relation *)malloc(sizeof(relation))) == NULL)
    {
        perror("Init pointer error");
        exit(-1);
    }
    return rel;
}

query_tuple * Init_Query_Tuple()
{
    query_tuple * qt;
    if((qt = (query_tuple *)malloc(sizeof(query_tuple))) == NULL)
    {
        perror("Query_Tuple error");
        exit(-1);
    }
    // qt[0].file1_ID = 0;
    // qt[0].file1_column = 0;
    // qt[0].rel = NULL;
    return qt;
}

uint64_t Find_Query_Tuple(query_tuple * qt, uint64_t file_ID,uint64_t counter)
{
    for (size_t i = 0; i < counter; i++)
    {
        if((qt[i].file1_ID == file_ID) && (qt[i].used != 0))// && (qt[i].file1_column == file_column))
        {
            return i;
        }
    }
    return TAG;
}

uint64_t Find_Query_Tuple_Predicate(query_tuple * qt, uint64_t file_ID,uint64_t counter)
{
    for (size_t i = 0; i < counter; i++)
    {
        if((qt[i].file1_ID == file_ID))// && (qt[i].file1_column == file_column))
        {
            return i;
        }
    }
    return TAG;
}


relation * Update_Relation(metadata * md, uint64_t md_pos,uint64_t array_pos, intervening * interv_final,uint64_t interv_pos, uint64_t payload_pos)
{
    uint64_t * ptr;
    //relation rel_struct;
    relation * rel;// = &rel_struct;
    interv_pos = 0;
    if(((rel = (relation *)malloc(sizeof(relation)))) == NULL)
    {
        perror("Create_Relation malloc");
        exit(-1);
    }

    // We'll create space for rows number of tuples
    if((rel -> tuples = (tuple *)malloc(interv_final -> final_rel[interv_pos].num_tuples * sizeof(tuple))) == NULL)
    {
        perror("Create_Relation.c , first malloc\n");
        exit(-1);
    }

    ptr =  md[md_pos].array[array_pos];

    for(size_t i = 0; i < interv_final -> final_rel[interv_pos].num_tuples; i++)
    {
        rel -> tuples[i].key = *(ptr + interv_final -> final_rel[interv_pos].tuples[i].payload[payload_pos]); // key is the value
        if((rel -> tuples[i].payload = (uint64_t *)malloc(sizeof(uint64_t))) == NULL)
        {
            perror("Create_Relation.c , first malloc\n");
            exit(-1);
        }
        rel -> tuples[i].payload[0] = interv_final -> final_rel[interv_pos].tuples[i].payload[payload_pos];// payload
        rel -> tuples[i].position = 1;

    }

    // The number of tuples is the number of rows
    rel -> num_tuples = interv_final -> final_rel[interv_pos].num_tuples;

    return rel;
}


void Print_Available_Filters(query_tuple * qt_filters, int64_t filter_counter)
{
    uint64_t flag = 0;
    printf("We can use these filters:\n");
    for (size_t z = 0; z < filter_counter; z++)
    {
        if(qt_filters[z].used == 0)
        {
            flag++;
            if(flag == filter_counter)
            {
                printf("None Available Filter!\n");

            }
        }
        else
        {
            printf("---%lu)%lu.%lu\n",z , qt_filters[z].file1_ID, qt_filters[z].file1_column);
        }
    }
}

void Execute_Queries(metadata * md, work_line * wl_ptr)
{
    uint64_t num_queries = wl_ptr -> num_predicates;

    uint64_t filter_counter = 0;
    uint64_t predicate_counter = 0;
    uint64_t rel_A_pos = 0;
    uint64_t rel_B_pos = 0;

    uint64_t rel_counter;

    // For every Query
    for (uint64_t i = 0; i < num_queries; i++)
    {
        printf("----------------------------------- QUERY = %lu --------------------------------\n",i);
        // for NULL
        if(i == 5 || i == 12 || i == 15 || i == 16|| i == 21)
        {
            continue;
        }
        for (uint64_t j = 0; j < wl_ptr -> predicates[i].num_tuples; j++)
        {
            printf("%lu.%lu = %lu.%lu\n",wl_ptr -> predicates[i].tuples[j].file1_ID, wl_ptr -> predicates[i].tuples[j].file1_column ,wl_ptr -> predicates[i].tuples[j].file2_ID, wl_ptr -> predicates[i].tuples[j].file2_column);
        }
        for (uint64_t j = 0; j < wl_ptr -> filters[i].num_tuples; j++)
        {
            printf("%lu.%lu\n",wl_ptr -> filters[i].tuples[j].file1_ID, wl_ptr -> filters[i].tuples[j].file1_column );
        }
          // exit(-1);

        intervening * interv_final = interveningInit();
        // variable to count how many rel are we going to use
        query_tuple * qt_filters;
        filter_counter = 0;

        query_tuple * qt_predicates;
        predicate_counter = 0;

        // Filters
        for (uint64_t j = 0; j < wl_ptr -> filters[i].num_tuples; j++)
        {
            // First time
            if(filter_counter == 0)
            {
                // Create Tuple
                if((qt_filters = Init_Query_Tuple()) == NULL)
                {
                    perror("Execute_Queries 1st malloc");
                    exit(-1);
                }
                // if(wl_ptr -> parameters[i].tuples[wl_ptr -> filters[i].tuples[j].file1_ID].file1_ID == wl_ptr -> parameters[i].tuples[wl_ptr -> filters[i].tuples[j].file2_ID].file1_ID)
                // {
                //     printf("EDW %lu, %lu\n",wl_ptr -> parameters[i].tuples[wl_ptr -> filters[i].tuples[j].file1_ID].file1_ID,wl_ptr -> parameters[i].tuples[wl_ptr -> filters[i].tuples[j].file2_ID].file1_ID);
                //     qt_filters[filter_counter].file1_ID = wl_ptr -> parameters[i].tuples[wl_ptr -> filters[i].tuples[j].file1_ID].file1_ID;
                //     qt_filters[filter_counter].file1_column = wl_ptr -> filters[i].tuples[j].file1_column;
                //
                //     qt_filters[filter_counter].used = 1;
                //
                //     qt_filters[filter_counter].rel = Create_Relation(md,wl_ptr -> parameters[i].tuples[wl_ptr -> filters[i].tuples[j].file1_ID].file1_ID, wl_ptr -> filters[i].tuples[j].file1_column);
                //     qt_filters[filter_counter].rel = Radix_Sort(qt_filters[filter_counter].rel);
                //
                //     relation * rel_R = Init_pointer();
                //
                //     rel_R = Create_Relation(md,wl_ptr -> parameters[i].tuples[wl_ptr -> filters[i].tuples[j].file2_ID].file1_ID, wl_ptr -> filters[i].tuples[j].file2_column);
                //     rel_R = Radix_Sort(rel_R);
                // //    Print_Relation_2(qt_filters -> rel);
                //
                //     if(!((qt_filters[filter_counter].rel, rel_R)))
                //     {
                //     //    Join_v2(interv_final, interv_final -> final_rel, qt_predicates[predicate_counter + 1].rel, qt_predicates[predicate_counter].file1_ID, qt_predicates[predicate_counter + 1].file1_ID);
                //         printf("None Results\n" );
                //     }
                //     else
                //     {
                //         qt_filters[filter_counter].rel = Radix_Sort(qt_filters[filter_counter].rel);
                //         Print_Relation_2(qt_filters[filter_counter].rel );
                //     }
                //
                //
                //
                //     if(!(qt_filters[filter_counter].rel = Scan(qt_filters[filter_counter].rel, rel_R)))
                //     {
                //     //    Join_v2(interv_final, interv_final -> final_rel, qt_predicates[predicate_counter + 1].rel, qt_predicates[predicate_counter].file1_ID, qt_predicates[predicate_counter + 1].file1_ID);
                //         printf("None Results\n" );
                //     }
                //     else
                //     {
                //         qt_filters[filter_counter].rel = Radix_Sort(qt_filters[filter_counter].rel);
                //         Print_Relation_2(qt_filters[filter_counter].rel );
                //     }
                //     filter_counter++;
                // //exit(-1);
                // }
                // else
                // {
                    // printf("IF : %lu.%lu\n",wl_ptr -> parameters[i].tuples[wl_ptr -> filters[i].tuples[j].file1_ID].file1_ID,  wl_ptr -> filters[i].tuples[j].file1_column);

                    printf("IF : %lu.%lu\n",wl_ptr -> filters[i].tuples[j].file1_ID,  wl_ptr -> filters[i].tuples[j].file1_column);
                    // qt_filters[filter_counter].file1_ID = wl_ptr -> parameters[i].tuples[wl_ptr -> filters[i].tuples[j].file1_ID].file1_ID;
                    qt_filters[filter_counter].file1_ID = wl_ptr -> filters[i].tuples[j].file1_ID;

                    qt_filters[filter_counter].file1_column = wl_ptr -> filters[i].tuples[j].file1_column;
                    qt_filters[filter_counter].used = 1;

                    qt_filters[filter_counter].rel = Create_Relation(md,wl_ptr -> parameters[i].tuples[wl_ptr -> filters[i].tuples[j].file1_ID].file1_ID,qt_filters[filter_counter].file1_column);
                    qt_filters[filter_counter].rel = Radix_Sort(qt_filters[filter_counter].rel);
                    qt_filters[filter_counter].rel = Filter(qt_filters[filter_counter].rel,wl_ptr -> filters[i].tuples[j].limit, wl_ptr -> filters[i].tuples[j].symbol);
                    filter_counter++;
                // }

            }
            else // if we have more than 1 filter // We have to check above case
            {
                uint64_t pos = TAG;
                if((pos = Find_Query_Tuple(qt_filters,wl_ptr -> filters[i].tuples[j].file1_ID, filter_counter)) != TAG)
                {
                    printf("Updated! %lu.%lu\n",wl_ptr -> filters[i].tuples[j].file1_ID,  wl_ptr -> filters[i].tuples[j].file1_column);
                    Update_Relation_Keys(md,wl_ptr -> parameters[i].tuples[wl_ptr -> filters[i].tuples[j].file1_ID].file1_ID,wl_ptr -> filters[i].tuples[j].file1_column,qt_filters[pos].rel,pos);
                    qt_filters[pos].rel = Radix_Sort(qt_filters[pos].rel);
                    qt_filters[pos].rel = Filter(qt_filters[pos].rel,wl_ptr -> filters[i].tuples[j].limit, wl_ptr -> filters[i].tuples[j].symbol);

                }
                else
                {
                    printf("ELSE : %lu.%lu\n",wl_ptr -> filters[i].tuples[j].file1_ID,  wl_ptr -> filters[i].tuples[j].file1_column);

                    if((qt_filters = (query_tuple *)realloc(qt_filters,sizeof(query_tuple) * (filter_counter + 1))) == NULL)
                    {
                        perror("Execute_Queries 1st realloc");
                        exit(-1);
                    }

                    qt_filters[filter_counter].file1_ID = wl_ptr -> filters[i].tuples[j].file1_ID;
                    qt_filters[filter_counter].file1_column = wl_ptr -> filters[i].tuples[j].file1_column;
                    qt_filters[filter_counter].used = 1;

                    qt_filters[filter_counter].rel = Create_Relation(md,wl_ptr -> parameters[i].tuples[wl_ptr -> filters[i].tuples[j].file1_ID].file1_ID,qt_filters[filter_counter].file1_column);
                    qt_filters[filter_counter].rel = Radix_Sort(qt_filters[filter_counter].rel);
                    qt_filters[filter_counter].rel = Filter(qt_filters[filter_counter].rel,wl_ptr -> filters[i].tuples[j].limit, wl_ptr -> filters[i].tuples[j].symbol);
                    filter_counter++;
                }

            }
        }


         printf("Filter number = %lu\n", filter_counter);
        // For predicates
        for (uint64_t j = 0; j < wl_ptr -> predicates[i].num_tuples; j++)
        {
            uint64_t posL = TAG;
            uint64_t posR = TAG;
            uint64_t Lcolumn_tmp;
            // if we don't have any filter
            if(predicate_counter == 0) // check if we have filter_rel == 0
            {
                printf("---------------------------- FIRST PREDICATE -------------------------------\n");
                uint64_t pos = TAG;
                if((qt_predicates = Init_Query_Tuple()) == NULL)
                {
                    perror("Execute_Queries 2nd malloc");
                    exit(-1);
                }

                for (size_t z = 0; z < filter_counter; z++)
                {
                    printf("%lu)%lu.%lu\n",z , qt_filters[z].file1_ID, qt_filters[z].file1_column);
                }
                // if you find file1_ID take this one!


                if((posL = Find_Query_Tuple(qt_filters, wl_ptr -> predicates[i].tuples[j].file1_ID, filter_counter)) != TAG)
                {
                        printf("Lpart)%lu.%lu has been found in filters!!!!\n",wl_ptr -> predicates[i].tuples[j].file1_ID,wl_ptr -> predicates[i].tuples[j].file1_column);
                        printf("Lpart)Updated from %lu.%lu to %lu.%lu\n",qt_filters[posL].file1_ID, qt_filters[posL].file1_column,wl_ptr -> predicates[i].tuples[j].file1_ID,  wl_ptr -> predicates[i].tuples[j].file1_column);
                        printf("Lpart)Updated! %lu.%lu\n",wl_ptr -> predicates[i].tuples[j].file1_ID,  wl_ptr -> predicates[i].tuples[j].file1_column);
                        Update_Relation_Keys(md,wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID,  wl_ptr -> predicates[i].tuples[j].file1_column,qt_filters[posL].rel,0);
                    //    Print_Relation_2(qt_filters[posL].rel);
                        qt_predicates[predicate_counter].file1_ID = wl_ptr -> predicates[i].tuples[j].file1_ID;
                        qt_predicates[predicate_counter].file1_column = wl_ptr -> predicates[i].tuples[j].file1_column;
                        printf("=------------------------------>%lu.%lu\n",qt_predicates[predicate_counter].file1_ID,qt_predicates[predicate_counter].file1_column);


                        qt_predicates[predicate_counter].rel = Init_pointer();
                        qt_predicates[predicate_counter].rel = qt_filters[posL].rel;

                        qt_predicates[predicate_counter].rel = Radix_Sort(qt_predicates[predicate_counter].rel);

                    //
                    //    free(qt_filters[posL].rel);
                        qt_filters[posL].used = 0;


                        Print_Available_Filters(qt_filters, filter_counter);

                }
                else // if you don't find you have to create it
                {
                    printf("Lpart)You have to create %lu.%lu!!!!\n",wl_ptr -> predicates[i].tuples[j].file1_ID,wl_ptr -> predicates[i].tuples[j].file1_column);
                    qt_predicates[predicate_counter].file1_ID = wl_ptr -> predicates[i].tuples[j].file1_ID;
                    qt_predicates[predicate_counter].file1_column = wl_ptr -> predicates[i].tuples[j].file1_column;
                    qt_predicates[predicate_counter].rel = Create_Relation(md,wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID,qt_predicates[predicate_counter].file1_column);
                    qt_predicates[predicate_counter].rel = Radix_Sort(qt_predicates[predicate_counter].rel);


                }
                pos = TAG;
                // if you find file2_ID take this one!
                // exit(-1);
                Print_Available_Filters(qt_filters, filter_counter);
                if((posR = Find_Query_Tuple(qt_filters, wl_ptr -> predicates[i].tuples[j].file2_ID, filter_counter )) != TAG)
                {
                    if((qt_predicates = (query_tuple *)realloc(qt_predicates,sizeof(query_tuple) * (predicate_counter + 2))) == NULL)
                    {
                        perror("Execute_Queries 1st realloc");
                        exit(-1);
                    }

                    printf("Rpart)%lu.%lu has been found!!!!\n",wl_ptr -> predicates[i].tuples[j].file2_ID,wl_ptr -> predicates[i].tuples[j].file2_column);

                    printf("Rpart)Updated from %lu.%lu to %lu.%lu\n",qt_filters[posR].file1_ID, qt_filters[posR].file1_column, wl_ptr -> predicates[i].tuples[j].file2_ID,  wl_ptr -> predicates[i].tuples[j].file2_column);
                    // Update_Relation_Keys(md,wl_ptr -> parameters[i].tuples[wl_ptr -> filters[i].tuples[j].file1_ID].file1_ID,wl_ptr -> filters[i].tuples[j].file1_column,qt_filters[posR].rel,0);

                    printf("Rpart)POS = %lu\n",posR);

                    Update_Relation_Keys(md,wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file2_ID].file1_ID,  wl_ptr -> predicates[i].tuples[j].file2_column,qt_filters[posR].rel,0);

                    qt_predicates[predicate_counter + 1].file1_ID = wl_ptr -> predicates[i].tuples[j].file2_ID;
                    qt_predicates[predicate_counter + 1].file1_column = wl_ptr -> predicates[i].tuples[j].file2_column;

                    printf("------------------------------>%lu\n",qt_predicates[predicate_counter + 1].file1_ID);

                    printf("------------------------------>%lu.%lu\n",qt_predicates[predicate_counter + 1].file1_ID,qt_predicates[predicate_counter + 1].file1_column);


                    qt_predicates[predicate_counter + 1].rel = Init_pointer();

                    qt_predicates[predicate_counter + 1].rel = qt_filters[posR].rel;

                    qt_predicates[predicate_counter + 1].rel = Radix_Sort(qt_predicates[predicate_counter + 1].rel);


                //    Print_Relation_2(qt_predicates[predicate_counter + 1].rel);
                    //    Print_Relation_2(qt_predicates[predicate_counter].rel);

                //    free(qt_filters[posR].rel);
                    qt_filters[posR].used = 0;

                    Print_Available_Filters(qt_filters, filter_counter);


                }
                else // if you don't find it create it
                {
                    if((qt_predicates = (query_tuple *)realloc(qt_predicates,sizeof(query_tuple) * (predicate_counter + 2))) == NULL)
                    {
                        perror("Execute_Queries 1st realloc");
                        exit(-1);
                    }

                    printf("Rpart)You have to create %lu.%lu!!!!\n",wl_ptr -> predicates[i].tuples[j].file2_ID,wl_ptr -> predicates[i].tuples[j].file2_column);
                    // if(i == 3)
                    // {
                    //     exit(-1);
                    // }
                    printf("We are at :%lu query\n",i);
                    //exit(-1);
                    // if(i ==3)
                    // Print_Relation_2(qt_predicates[predicate_counter].rel);
                    qt_predicates[predicate_counter + 1].file1_ID = wl_ptr -> predicates[i].tuples[j].file2_ID;
                    qt_predicates[predicate_counter + 1].file1_column = wl_ptr -> predicates[i].tuples[j].file2_column;
                    printf("Rpart)%lu.%lu------->\n",qt_predicates[predicate_counter + 1].file1_ID,qt_predicates[predicate_counter + 1].file1_column);

                    qt_predicates[predicate_counter + 1].rel = Create_Relation(md, wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file2_ID].file1_ID,qt_predicates[predicate_counter + 1].file1_column);
                    qt_predicates[predicate_counter + 1].rel = Radix_Sort(qt_predicates[predicate_counter + 1].rel);

                    //Print_Relation_2(qt_predicates[predicate_counter + 1].rel );


                    Print_Available_Filters(qt_filters, filter_counter);
                }
                printf("RowIDs : %lu|%lu\n", qt_predicates[predicate_counter].file1_ID, qt_predicates[predicate_counter + 1].file1_ID);

                // Check 1st predicate for self join or scan
                if(!Join_v2(interv_final, qt_predicates[predicate_counter].rel, qt_predicates[predicate_counter + 1].rel, qt_predicates[predicate_counter].file1_ID, qt_predicates[predicate_counter + 1].file1_ID))
                {
                //    Join_v2(interv_final, interv_final -> final_rel, qt_predicates[predicate_counter + 1].rel, qt_predicates[predicate_counter].file1_ID, qt_predicates[predicate_counter + 1].file1_ID);
                    printf("None Results\n" );
                }
                else
                {
                //    Print_Relation_2(interv_final -> final_rel);
                    printf("---------> %lu\n",interv_final -> final_rel -> num_tuples);
                    for (size_t z = 0; z < interv_final -> position; z++)
                    {
                    //    printf("=--------> %lu\n",qt_predicates[z].rel -> num_tuples);
                        qt_predicates[z].rel = Update_Predicates(interv_final -> final_rel, z);
                        printf("=--------> %lu\n",qt_predicates[z].rel -> num_tuples);
                    //    Print_Relation_2(qt_predicates[z].rel);
                    }
                }

                predicate_counter+=2;

            }

            else        // Rest predicates
            {

                printf("-------------------- REST PREDICATES----------------------\n" );

                uint64_t Lflag = 0;
                uint64_t Rflag = 0;
                // Lpart of predicate
                if((posL = Find_Query_Tuple(qt_filters, wl_ptr -> predicates[i].tuples[j].file1_ID, filter_counter)) != TAG)
                {
                    if((qt_predicates = (query_tuple *)realloc(qt_predicates,sizeof(query_tuple) * (predicate_counter + 1))) == NULL)
                    {
                        perror("Execute_Queries 1st realloc");
                        exit(-1);
                    }
                    printf("Lpart)  %lu.%lu has been found in filters!!!!\n",wl_ptr -> predicates[i].tuples[j].file1_ID,wl_ptr -> predicates[i].tuples[j].file1_column);
                    printf("Lpart)  Updated from %lu.%lu to %lu.%lu\n",qt_filters[posL].file1_ID, qt_filters[posL].file1_column, wl_ptr -> predicates[i].tuples[j].file1_ID,  wl_ptr -> predicates[i].tuples[j].file1_column);
                    printf("Lpart)  Updated! %lu.%lu\n",wl_ptr -> predicates[i].tuples[j].file1_ID,  wl_ptr -> predicates[i].tuples[j].file1_column);

                    Update_Relation_Keys(md,wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID,  wl_ptr -> predicates[i].tuples[j].file1_column,qt_filters[posL].rel,0);
                    qt_predicates[predicate_counter].file1_ID = wl_ptr -> predicates[i].tuples[j].file1_ID;
                    qt_predicates[predicate_counter].file1_column = wl_ptr -> predicates[i].tuples[j].file1_column;
                    qt_predicates[predicate_counter].rel = Init_pointer();
                    qt_predicates[predicate_counter].rel = qt_filters[posL].rel;
                    qt_predicates[predicate_counter].rel = Radix_Sort(qt_predicates[predicate_counter].rel);

                    qt_filters[posL].used = 0;
                    predicate_counter++;
                    Lflag = 1;
                }
                else // if you don't find you have to create it
                {
                    // Check Lpart of predicate
                    if((posL = Find_Query_Tuple_Predicate(qt_predicates, wl_ptr -> predicates[i].tuples[j].file1_ID, predicate_counter)) != TAG)
                    {
                        printf("Lpart)  I have found it in predicates:%lu.%lu\n",wl_ptr -> predicates[i].tuples[j].file1_ID, wl_ptr -> predicates[i].tuples[j].file1_column);
                    //    Print_Relation_2(qt_predicates[0].rel);
                        //Print_Relation_2(qt_filters[0].rel);
                        //Print_Available_Filters(qt_filters, filter_counter);

                        Update_Relation_Keys(md,wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID,  wl_ptr -> predicates[i].tuples[j].file1_column,qt_predicates[posL].rel,0);
                        qt_predicates[posL].file1_ID = wl_ptr -> predicates[i].tuples[j].file1_ID;

                        qt_predicates[posL].file1_column = wl_ptr -> predicates[i].tuples[j].file1_column;
                        printf("Lpart)  I have found it %lu.%lu-> %lu\n",qt_predicates[posL].file1_ID, qt_predicates[posL].file1_column,posL);
                        qt_predicates[posL].rel = Radix_Sort(qt_predicates[posL].rel);
                        Lflag = 2;
                        Lcolumn_tmp = qt_predicates[posL].file1_column;
                        //  exit(-1);
                        //
                        //Print_Relation_2(qt_predicates[pos].rel);
                    }
                    else
                    {
                        if((qt_predicates = (query_tuple *)realloc(qt_predicates,sizeof(query_tuple) * (predicate_counter + 1))) == NULL)
                        {
                            perror("Execute_Queries 1st realloc");
                            exit(-1);
                        }
                        printf("Lpart)  You have to create %lu.%lu!!!!\n",wl_ptr -> predicates[i].tuples[j].file1_ID,wl_ptr -> predicates[i].tuples[j].file1_column);
                        qt_predicates[predicate_counter].file1_ID = wl_ptr -> predicates[i].tuples[j].file1_ID;
                        qt_predicates[predicate_counter].file1_column = wl_ptr -> predicates[i].tuples[j].file1_column;
                        qt_predicates[predicate_counter].rel = Create_Relation(md,wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID,qt_predicates[predicate_counter].file1_column);
                        qt_predicates[predicate_counter].rel = Radix_Sort(qt_predicates[predicate_counter].rel);
                        posL = predicate_counter;
                        // Print_Relation_2(qt_predicates[2].rel);
                        predicate_counter++;
                        Lflag = 3;
                    }

                }



                // Rpart of predicate
                if((posR = Find_Query_Tuple(qt_filters, wl_ptr -> predicates[i].tuples[j].file2_ID, filter_counter)) != TAG)
                {
                    if((qt_predicates = (query_tuple *)realloc(qt_predicates,sizeof(query_tuple) * (predicate_counter + 1))) == NULL)
                    {
                        perror("Execute_Queries 1st realloc");
                        exit(-1);
                    }
                    printf("Rpart)  %lu.%lu has been found in filters!!!!\n",wl_ptr -> predicates[i].tuples[j].file2_ID,wl_ptr -> predicates[i].tuples[j].file2_column);
                    printf("Rpart)  Updated from %lu.%lu to %lu.%lu\n",qt_filters[posR].file1_ID, qt_filters[posR].file1_column, wl_ptr -> predicates[i].tuples[j].file2_ID,  wl_ptr -> predicates[i].tuples[j].file2_column);
                    printf("Rpart)  Updated! %lu.%lu\n",wl_ptr -> predicates[i].tuples[j].file2_ID,  wl_ptr -> predicates[i].tuples[j].file2_column);

                    Update_Relation_Keys(md,wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file2_ID].file1_ID,  wl_ptr -> predicates[i].tuples[j].file2_column,qt_filters[posR].rel,0);
                    qt_predicates[predicate_counter].file1_ID = wl_ptr -> predicates[i].tuples[j].file2_ID;
                    qt_predicates[predicate_counter].file1_column = wl_ptr -> predicates[i].tuples[j].file2_column;
                    qt_predicates[predicate_counter].rel = Init_pointer();
                    qt_predicates[predicate_counter].rel = qt_filters[posR].rel;
                    qt_predicates[predicate_counter].rel = Radix_Sort(qt_predicates[predicate_counter].rel);
                    printf("POSSSSS = %lu\n",posR);
                    //free(qt_filters[posR].rel);
                    posR = predicate_counter;
                    qt_filters[posR].used = 0;
                    predicate_counter++;
                    Rflag = 1;

                }
                else // if you don't find you have to create it
                {
                    // Check Rpart of predicate
                    if((posR = Find_Query_Tuple_Predicate(qt_predicates, wl_ptr -> predicates[i].tuples[j].file2_ID, predicate_counter)) != TAG)
                    {
                        printf("Rpart)I have found it %lu.%lu\n",wl_ptr -> predicates[i].tuples[j].file2_ID, wl_ptr -> predicates[i].tuples[j].file2_column);

                    //    Print_Available_Filters(qt_filters, filter_counter);
                        Update_Relation_Keys(md,wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file2_ID].file1_ID,  wl_ptr -> predicates[i].tuples[j].file2_column,qt_predicates[posR].rel,0);
                        qt_predicates[posR].file1_ID = wl_ptr -> predicates[i].tuples[j].file2_ID;
                        qt_predicates[posR].file1_column = wl_ptr -> predicates[i].tuples[j].file2_column;
                        printf("->part)I have found it %lu.%lu\n",qt_predicates[posR].file1_ID, qt_predicates[posR].file1_column);
                        printf("-L>part)I have found it %lu.%lu-->%lu\n",qt_predicates[posL].file1_ID, qt_predicates[posL].file1_column,posL);
                        qt_predicates[posR].rel = Radix_Sort(qt_predicates[posR].rel);


                        Rflag = 2;

                    }
                    else
                    {
                        if((qt_predicates = (query_tuple *)realloc(qt_predicates,sizeof(query_tuple) * (predicate_counter + 1))) == NULL)
                        {
                            perror("Execute_Queries 1st realloc");
                            exit(-1);
                        }
                        printf("Rpart)  You have to create %lu.%lu!!!!\n",wl_ptr -> predicates[i].tuples[j].file2_ID,wl_ptr -> predicates[i].tuples[j].file2_column);
                        qt_predicates[predicate_counter].file1_ID = wl_ptr -> predicates[i].tuples[j].file2_ID;
                        qt_predicates[predicate_counter].file1_column = wl_ptr -> predicates[i].tuples[j].file2_column;
                        qt_predicates[predicate_counter].rel = Create_Relation(md,wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file2_ID].file1_ID,qt_predicates[predicate_counter].file1_column);
                        qt_predicates[predicate_counter].rel = Radix_Sort(qt_predicates[predicate_counter].rel);
                        posR = predicate_counter;
                        // Print_Relation_2(qt_predicates[predicate_counter].rel);
                        predicate_counter++;

                        Rflag = 3;
                    }
                }
                uint64_t pos = TAG;
                // Join
                if(Lflag == 1 && Rflag == 1)
                {
                    printf("Case: 1,1!!\n");
                    exit(-1);
                }
                else if(Lflag == 1 && Rflag == 2)    // intervening is going to take Rpart
                {
                    printf("Case: 1,2!!\n");
                    exit(-1);
                }
                else if(Lflag == 1 && Rflag == 3)
                {
                    printf("Case: 1,3!!\n");
                    exit(-1);
                }
                if(Lflag == 2 && Rflag == 1) //  intervening is going to take Lpart
                {
                    printf("Case: 2,1!!\n");
                    printf("%lu\n",interv_final -> final_rel ->num_tuples);

                    printf("%lu\n",qt_predicates[posR].rel ->num_tuples);
                    pos = FindRowID(interv_final, qt_predicates[posL].file1_ID);
                    printf("position in interv = %lu\n",pos);
                //    Print_Relation_2(interv_final -> final_rel);

                    Update_Relation_Keys(md, qt_predicates[posL].file1_ID, qt_predicates[posL].file1_column, interv_final -> final_rel, pos);
                    interv_final -> final_rel = Radix_Sort(interv_final -> final_rel);

                    printf("%lu.%lu=%lu.%lu\n",qt_predicates[posL].file1_ID,qt_predicates[posL].file1_column,qt_predicates[posR].file1_ID,qt_predicates[posR].file1_column);
                    Print_Relation_2(qt_predicates[posR].rel);
                    Print_Relation_2(interv_final -> final_rel);
                    if(!Join_v2(interv_final, interv_final -> final_rel, qt_predicates[posR].rel, qt_predicates[posL].file1_ID, qt_predicates[posR].file1_ID))
                    {
                    //    Join_v2(interv_final, interv_final -> final_rel, qt_predicates[predicate_counter + 1].rel, qt_predicates[predicate_counter].file1_ID, qt_predicates[predicate_counter + 1].file1_ID);
                        printf("None Results\n" );
                    }
                    else
                    {
                        Print_Relation_2(interv_final -> final_rel);
                    //qt_predicates[posR].rel = Update_Predicates(interv_final -> final_rel, pos);
                //    Print_Relation_2(qt_predicates[posR].rel);

                        for (size_t z = 0; z < interv_final -> position; z++)
                        {
                            qt_predicates[z].rel = Update_Predicates(interv_final -> final_rel, z);
                        //    Print_Relation_2(qt_predicates[z].rel);
                        }

                    }
                    printf("%lu\n",qt_predicates[posR].rel ->num_tuples);

                //    exit(-1);
                }
                else if(Lflag == 2 && Rflag == 2)    // SELF JOIN
                {
                    printf("Case: 2,2!!,%lu , %lu\n",posL , posR);
                    exit(-1);

                    printf("->%lu.%lu=%lu.%lu\n",qt_predicates[posL].file1_ID,Lcolumn_tmp,qt_predicates[posR].file1_ID,qt_predicates[posR].file1_column);
                    // for example 0 1 | 0.1 = 0.2
                    if(wl_ptr -> predicates[i].tuples[j].file1_ID == wl_ptr -> predicates[i].tuples[j].file2_ID)
                    {

                        printf("SARWSHHHH\n");
                        exit(-1);
                        if(!Self_Join(interv_final, interv_final -> final_rel, qt_predicates[posR].rel, qt_predicates[posL].file1_ID, qt_predicates[posR].file1_ID))
                        {
                        //    Join_v2(interv_final, interv_final -> final_rel, qt_predicates[predicate_counter + 1].rel, qt_predicates[predicate_counter].file1_ID, qt_predicates[predicate_counter + 1].file1_ID);
                            printf("None Results\n" );
                        }
                        else
                        {
                            Print_Relation_2(interv_final -> final_rel);

                        }
                    }

                    // for example   0.1 = 1.2
                    else
                    {
                        for (size_t z = 0; z < interv_final -> position; z++)
                        {
                            printf("%lu|", interv_final -> rowId[z]);
                        }
                        printf("\n" );
                        // for example 0 0 | 0.1 = 1.2
                        if(wl_ptr -> parameters[i].tuples[wl_ptr-> predicates[i].tuples[j].file1_ID].file1_ID == wl_ptr ->parameters[i].tuples[wl_ptr-> predicates[i].tuples[j].file2_ID].file1_ID)
                        {
                            printf("%lu\n",qt_predicates[posR].rel ->num_tuples);
                        //    Print_Relation_2(qt_predicates[posR].rel);
                            pos = FindRowID(interv_final, qt_predicates[posL].file1_ID);
                            Update_Relation_Keys(md, qt_predicates[posL].file1_ID, Lcolumn_tmp, interv_final -> final_rel, pos);
                            // Print_Relation_2(qt_predicates[posR].rel);
                            interv_final -> final_rel = Radix_Sort(interv_final -> final_rel);

                            printf("->%lu.%lu=%lu.%lu\n",qt_predicates[posL].file1_ID,Lcolumn_tmp,qt_predicates[posR].file1_ID,qt_predicates[posR].file1_column);
                            Print_Relation_2( qt_predicates[posR].rel);
                            // qt_predicates[posR].rel = Update_Predicates(interv_final -> final_rel, pos);
                            // Update_Relation_Keys(md, qt_predicates[posR].file1_ID, qt_predicates[posR].file1_column, qt_predicates[posR].rel, 0);
                            //  qt_predicates[posR].rel = Radix_Sort(qt_predicates[posR].rel);


                            //Print_Relation_2(qt_predicates[posR].rel);
                            // Update_Relation_Keys(md, qt_predicates[posR].file1_ID, qt_predicates[posR].file1_column, qt_predicates[posR].rel, 0);
                            // qt_predicates[posR].rel = Radix_Sort(qt_predicates[posR].rel);
                            printf("%lu\n",interv_final -> final_rel ->num_tuples);
                            printf("%lu\n",qt_predicates[posR].rel ->num_tuples);
                            if(!Join_v2(interv_final, interv_final -> final_rel, qt_predicates[posR].rel, qt_predicates[posL].file1_ID, qt_predicates[posR].file1_ID))
                            {
                            //    Join_v2(interv_final, interv_final -> final_rel, qt_predicates[predicate_counter + 1].rel, qt_predicates[predicate_counter].file1_ID, qt_predicates[predicate_counter + 1].file1_ID);
                                printf("None Results\n" );
                            }
                            else
                            {
                                // Print_Relation_2(interv_final -> final_rel);
                            }
                        }
                        else
                        {
                            pos = FindRowID(interv_final, qt_predicates[posL].file1_ID);
                            Update_Relation_Keys(md, qt_predicates[posL].file1_ID, Lcolumn_tmp, interv_final -> final_rel, pos);
                            if(!Self_Join(interv_final, interv_final -> final_rel, qt_predicates[posR].rel, qt_predicates[posL].file1_ID, qt_predicates[posR].file1_ID))
                            {
                            //    Join_v2(interv_final, interv_final -> final_rel, qt_predicates[predicate_counter + 1].rel, qt_predicates[predicate_counter].file1_ID, qt_predicates[predicate_counter + 1].file1_ID);
                                printf("None Results\n" );
                            }
                            else
                            {

                                Print_Relation_2(interv_final -> final_rel);

                            }
                        }

                    }
                    //
                    // if(i == 19 || i == 22 || i == 23 || i == 27 || i == 34 || i == 37 || i == 48)
                    // {
                    //     if(!Self_Join(interv_final, interv_final -> final_rel, qt_predicates[posR].rel, qt_predicates[posL].file1_ID, qt_predicates[posR].file1_ID))
                    //     {
                    //     //    Join_v2(interv_final, interv_final -> final_rel, qt_predicates[predicate_counter + 1].rel, qt_predicates[predicate_counter].file1_ID, qt_predicates[predicate_counter + 1].file1_ID);
                    //         printf("None Results\n" );
                    //     }
                    //     else
                    //     {
                    //     //    Print_Relation_2(interv_final -> final_rel);
                    //     }
                    //
                    // }
                    // else
                    // if(!Join_v2(interv_final, interv_final -> final_rel, qt_predicates[posR].rel, qt_predicates[posL].file1_ID, qt_predicates[posR].file1_ID))
                    // {
                    // //    Join_v2(interv_final, interv_final -> final_rel, qt_predicates[predicate_counter + 1].rel, qt_predicates[predicate_counter].file1_ID, qt_predicates[predicate_counter + 1].file1_ID);
                    //     printf("None Results\n" );
                    // }
                    // else
                    // {
                    // //    Print_Relation_2(interv_final -> final_rel);
                    // }
                    // //    Print_
                    //
                    // // continue;
                    // //exit(-1);
                }
                //  rowid 3|0
                // 3.1 = 1.0
                else if(Lflag == 2 && Rflag == 3)  //   intervening is going to take Lpart
                {
                    printf("%lu\n",interv_final -> final_rel ->num_tuples);

                    printf("%lu\n",qt_predicates[posR].rel ->num_tuples);
                    printf("Eimaste edww ---> %lu\n",posL );
                    pos = FindRowID(interv_final, qt_predicates[posL].file1_ID);


                    printf("Eimaste edww ---> %lu\n",pos);
                    Update_Relation_Keys(md, wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID, wl_ptr -> predicates[i].tuples[j].file1_column, interv_final -> final_rel, pos);
                    interv_final -> final_rel = Radix_Sort(interv_final -> final_rel);

                    printf("%lu.%lu=%lu.%lu\n",qt_predicates[posL].file1_ID,qt_predicates[posL].file1_column,qt_predicates[posR].file1_ID,qt_predicates[posR].file1_column);
                    if(!Join_v2(interv_final, interv_final -> final_rel, qt_predicates[posR].rel, qt_predicates[posL].file1_ID, qt_predicates[posR].file1_ID))
                    {
                    //    Join_v2(interv_final, interv_final -> final_rel, qt_predicates[predicate_counter + 1].rel, qt_predicates[predicate_counter].file1_ID, qt_predicates[predicate_counter + 1].file1_ID);
                        printf("None Results\n" );
                    }
                    else
                    {
                    //    Print_Relation_2(interv_final -> final_rel);
                        printf("%lu\n",interv_final -> final_rel ->num_tuples);
                    //qt_predicates[posR].rel = Update_Predicates(interv_final -> final_rel, pos);
                //    Print_Relation_2(qt_predicates[posR].rel);

                        // for (size_t z = 0; z < interv_final -> position; z++)
                        // {
                        //     qt_predicates[z].rel = Update_Predicates(interv_final -> final_rel, z);
                        // //    Print_Relation_2(qt_predicates[z].rel);
                        // }

                    }
                    printf("%lu\n",qt_predicates[posR].rel ->num_tuples);
                    // Print_Relation_2(interv_final -> final_rel);

                //    continue;
                }
                if(Lflag == 3 && Rflag == 1)
                {
                    printf("Case: 3,1!!\n");
                    exit(-1);
                }
                else if(Lflag == 3 && Rflag == 2)   //  intervening is going to take Rpart
                {
                    printf("Case: 3,2!!\n");
                    exit(-1);
                }
                else if(Lflag == 3 && Rflag == 3)    //
                {
                    printf("Case: 3,3!!\n");
                    exit(-1);
                }
                Lflag = 0;
                Rflag = 0;
                posL = TAG;
                posR = TAG;
                pos = TAG;

            }



        }


    //    exit(-1);



        printf("%lu)",i);
        for (uint64_t z = 0; z < wl_ptr -> selects[i].num_tuples; z++) // for every select
        {
            for (uint64_t x = 0; x < interv_final -> position; x++)   // Search in interval final in rowid
            {

                if(interv_final -> rowId[x] == wl_ptr -> selects[i].tuples[z].file1_ID)
                {

                    CheckSum(md,wl_ptr -> parameters[i].tuples[wl_ptr -> selects[i].tuples[z].file1_ID].file1_ID,wl_ptr -> selects[i].tuples[z].file1_column,interv_final -> final_rel,x);

                    break;
                }
            }
        }
           // exit(-1);


        printf("\n %lu--\n",filter_counter);
        for (size_t z = 0; z < filter_counter; z++)
        {

            free(qt_filters[z].rel);
        }

        free(qt_filters);
        for (size_t z = 0; z < predicate_counter; z++)
        {
            /* code */
            free(qt_predicates[z].rel);
        }
        free(qt_predicates);
        free(interv_final -> final_rel);
        free(interv_final);
        filter_counter = 0;
        predicate_counter = 0;
        printf("-------------------------------------------------------------------\n",i);
    }



}
