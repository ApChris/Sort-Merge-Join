#include "../include/executeQuery.h"
#include "../include/bestTree.h"
uint64_t CheckSum(metadata * md, uint64_t md_row, uint64_t md_column, relation * rel, uint64_t pos)
{
    uint64_t * ptr = (uint64_t *)md[md_row].array[md_column];
    uint64_t sum = 0;
    for (uint64_t i = 0; i < rel -> num_tuples; i++)
    {
        sum += *(ptr + rel -> tuples[i].payload[pos]); // calculate keys
        //sum += rel -> tuples[i].payload[pos]; // calculate payloads
    }
    if(sum == 0)
    {
        printf("NULL");
    }
    else
    {
        printf("%lu",sum);
    }
    return sum;
}

// relation * Copy(relation * rel)
// {
//     relation * rel_final  = Init_pointer();
//     rel_final -> num_tuples = rel -> final_tuples;
//
// }

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
    for (uint64_t i = 0; i < counter; i++)
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
    for (uint64_t i = 0; i < counter; i++)
    {
        if((qt[i].file1_ID == file_ID))// && (qt[i].file1_column == file_column))
        {
            return i;
        }
    }
    return TAG;
}



void Print_Available_Filters(query_tuple * qt_filters, uint64_t filter_counter)
{
    uint64_t flag = 0;
    printf("We can use these filters:\n");
    for (uint64_t z = 0; z < filter_counter; z++)
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

void Execute_Queries(metadata * md, work_line * wl_ptr,uint64_t query, statistics * stats, char c)
{
    // uint64_t num_queries = wl_ptr -> num_predicates;

    uint64_t filter_counter = 0;
    uint64_t predicate_counter = 0;
    uint64_t null_flag = 0;
    uint64_t null_flag_Join = 0;
    uint64_t pos = TAG;

    // For every Query
    for (uint64_t i = query; i < query + 1; i++)
    {
        pos = TAG;
        // printf("----------------------------------- QUERY = %lu --------------------------------\n",i);
        //
        // for (uint64_t j = 0; j < wl_ptr -> predicates[i].num_tuples; j++)
        // {
        //     printf("%lu.%lu = %lu.%lu\n",wl_ptr -> predicates[i].tuples[j].file1_ID, wl_ptr -> predicates[i].tuples[j].file1_column ,wl_ptr -> predicates[i].tuples[j].file2_ID, wl_ptr -> predicates[i].tuples[j].file2_column);
        // }
        // for (uint64_t j = 0; j < wl_ptr -> filters[i].num_tuples; j++)
        // {
        //     printf("%lu.%lu%c%lu\n",wl_ptr -> filters[i].tuples[j].file1_ID, wl_ptr -> filters[i].tuples[j].file1_column,wl_ptr -> filters[i].tuples[j].symbol,wl_ptr -> filters[i].tuples[j].limit );
        // }
        //   // exit(-1);
        statistics * query_stats = Init_Query_Stats(stats,wl_ptr, query);
        Update_Query_Stats(query_stats,  wl_ptr, query);
        bestTree * bt = Join_Enum(query_stats, wl_ptr, query);
        // printf("Best cost is %lf\nBest path is %lu\n",bt -> cost,bt -> path);
        // free(bt);
        for (uint64_t j = 0; j <  wl_ptr -> parameters[i].num_tuples; j++)
        {
            free(query_stats[j].la);
            free(query_stats[j].ua);
            free(query_stats[j].fa);
            free(query_stats[j].size_da);
            free(query_stats[j].da);
        }
        free(query_stats);


        // exit(-1);
        intervening * interv_final = interveningInit();
        // variable to count how many rel are we going to use
        query_tuple * qt_filters;
        filter_counter = 0;

        query_tuple * qt_predicates;
        predicate_counter = 0;

        null_flag = 0;
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

                qt_filters[filter_counter].file1_ID = wl_ptr -> filters[i].tuples[j].file1_ID;

                qt_filters[filter_counter].file1_column = wl_ptr -> filters[i].tuples[j].file1_column;
                qt_filters[filter_counter].used = 1;
                qt_filters[filter_counter].rel = Create_Relation(md,wl_ptr -> parameters[i].tuples[wl_ptr -> filters[i].tuples[j].file1_ID].file1_ID,qt_filters[filter_counter].file1_column);
                qt_filters[filter_counter].rel = Radix_Sort(qt_filters[filter_counter].rel);
                qt_filters[filter_counter].rel = Filter(qt_filters[filter_counter].rel,wl_ptr -> filters[i].tuples[j].limit, wl_ptr -> filters[i].tuples[j].symbol);

                filter_counter++;

            }
            else // if we have more than 1 filter // We have to check above case
            {
                if((pos = Find_Query_Tuple(qt_filters,wl_ptr -> filters[i].tuples[j].file1_ID, filter_counter)) != TAG)
                {
                    Update_Relation_Keys(md,wl_ptr -> parameters[i].tuples[wl_ptr -> filters[i].tuples[j].file1_ID].file1_ID,wl_ptr -> filters[i].tuples[j].file1_column,qt_filters[pos].rel,pos);
                    qt_filters[pos].rel = Radix_Sort(qt_filters[pos].rel);
                    qt_filters[pos].rel = Filter(qt_filters[pos].rel,wl_ptr -> filters[i].tuples[j].limit, wl_ptr -> filters[i].tuples[j].symbol);

                }
                else
                {

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

        // For predicates
        for (uint64_t pred = 0; pred < wl_ptr -> predicates[i].num_tuples; pred++)
        {

            for (size_t f = 0; f < filter_counter; f++)
            {
                if(!qt_filters[f].rel -> num_tuples)
                {
                    null_flag = 1;
                }
            }
            if(null_flag)
            {
                break;
            }
            uint64_t j = bt -> path;
            if(c == 's')
            {
                if( i == 22)
                {

                    j = pred;

                }
                else
                {
                    j = Current_Best_Predicate(wl_ptr -> predicates[i].num_tuples,pred,j);
                }
            }
            else
            {
                if(i == 10 || i == 15 || i == 19 || i == 38)
                {
                    j = pred;

                }
                else
                {
                    j = Current_Best_Predicate(wl_ptr -> predicates[i].num_tuples,pred,j);
                }
            }



            uint64_t posL = TAG;
            uint64_t posR = TAG;
            uint64_t Lcolumn_tmp;
            // if we don't have any filter
            if(predicate_counter == 0) // check if we have filter_rel == 0
            {
                // printf("---------------------------- FIRST PREDICATE -------------------------------\n");

                if((qt_predicates = Init_Query_Tuple()) == NULL)
                {
                    perror("Execute_Queries 2nd malloc");
                    exit(-1);
                }

                // Lpart of current predicate has been found in filters
                if((posL = Find_Query_Tuple(qt_filters, wl_ptr -> predicates[i].tuples[j].file1_ID, filter_counter)) != TAG)
                {

                    if(qt_filters[posL].rel -> num_tuples != 0)
                    {

                        // printf("Prin to update %lu %lu, tuples : %lu\n",wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID,  wl_ptr -> predicates[i].tuples[j].file1_column,qt_filters[posL].rel ->num_tuples);
                        Update_Relation_Keys(md,wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID,  wl_ptr -> predicates[i].tuples[j].file1_column,qt_filters[posL].rel,0);
                        // printf("Prin to update %lu %lu, tuples : %lu\n",wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID,  wl_ptr -> predicates[i].tuples[j].file1_column,qt_filters[posL].rel ->num_tuples);
                        qt_predicates[predicate_counter].file1_ID = wl_ptr -> predicates[i].tuples[j].file1_ID;
                        qt_predicates[predicate_counter].file1_column = wl_ptr -> predicates[i].tuples[j].file1_column;


                        //qt_predicates[predicate_counter].rel = Init_pointer();

                        qt_predicates[predicate_counter].rel = Update_Predicates(qt_filters[posL].rel,0);

                        if(qt_predicates[predicate_counter].rel == NULL)
                        {
                            printf("Exit because I have found an empty filter\n");
                            exit(-1);
                        }
                        // printf("edw\n");

                        qt_predicates[predicate_counter].rel = Radix_Sort(qt_predicates[predicate_counter].rel);


                        // for (uint64_t z = 0; z < filter_counter; z++)
                        // {
                        //     for (size_t k = 0; k < qt_filters[z].rel -> num_tuples; k++)
                        //     {
                        //         free(qt_filters[z].rel -> tuples[k].payload);
                        //     }
                        //     free(qt_filters[z].rel -> tuples);
                        //     free(qt_filters[z].rel);
                        // }
                        // free(qt_filters);
                        //
                        // for (uint64_t z = 0; z < 1; z++)
                        // {
                        //     for (size_t k = 0; k < qt_predicates[z].rel -> num_tuples; k++)
                        //     {
                        //         free(qt_predicates[z].rel -> tuples[k].payload);
                        //     }
                        //     free(qt_predicates[z].rel -> tuples);
                        //     free(qt_predicates[z].rel);
                        // }
                        // free(qt_predicates);
                        // return;
                        qt_filters[posL].used = 0;

                    }
                    else
                    {
                        // qt_filters[filter_counter].rel = Create_Relation(md,wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID,qt_predicates[predicate_counter].file1_column);
                        // qt_predicates[predicate_counter].rel = Radix_Sort(qt_predicates[predicate_counter].rel);
                        qt_predicates[predicate_counter].rel = NULL;
                        null_flag = 1;
                        break;
                    }


                }
                else // if you don't find you have to create it
                {
                    qt_predicates[predicate_counter].file1_ID = wl_ptr -> predicates[i].tuples[j].file1_ID;
                    qt_predicates[predicate_counter].file1_column = wl_ptr -> predicates[i].tuples[j].file1_column;
                    qt_predicates[predicate_counter].rel = Create_Relation(md,wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID,qt_predicates[predicate_counter].file1_column);
                    qt_predicates[predicate_counter].rel = Radix_Sort(qt_predicates[predicate_counter].rel);


                }
                pos = TAG;

                if((posR = Find_Query_Tuple(qt_filters, wl_ptr -> predicates[i].tuples[j].file2_ID, filter_counter )) != TAG)
                {

                    if((qt_predicates = (query_tuple *)realloc(qt_predicates,sizeof(query_tuple) * (predicate_counter + 2))) == NULL)
                    {
                        perror("Execute_Queries 1st realloc");
                        exit(-1);
                    }
                    if(qt_filters[posR].rel -> num_tuples != 0)
                    {
                        Update_Relation_Keys(md,wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file2_ID].file1_ID,  wl_ptr -> predicates[i].tuples[j].file2_column,qt_filters[posR].rel,0);

                        qt_predicates[predicate_counter + 1].file1_ID = wl_ptr -> predicates[i].tuples[j].file2_ID;
                        qt_predicates[predicate_counter + 1].file1_column = wl_ptr -> predicates[i].tuples[j].file2_column;

                        qt_predicates[predicate_counter + 1].rel = Update_Predicates(qt_filters[posR].rel, 0);

                        qt_predicates[predicate_counter + 1].rel = Radix_Sort(qt_predicates[predicate_counter + 1].rel);

                        qt_filters[posR].used = 0;

                    }
                    else
                    {
                        qt_predicates[predicate_counter + 1].rel = NULL;
                        null_flag = 1;
                        break;
                    }

                }
                else // if you don't find it create it
                {
                    if((qt_predicates = (query_tuple *)realloc(qt_predicates,sizeof(query_tuple) * (predicate_counter + 2))) == NULL)
                    {
                        perror("Execute_Queries 1st realloc");
                        exit(-1);
                    }

                    qt_predicates[predicate_counter + 1].file1_ID = wl_ptr -> predicates[i].tuples[j].file2_ID;
                    qt_predicates[predicate_counter + 1].file1_column = wl_ptr -> predicates[i].tuples[j].file2_column;

                    qt_predicates[predicate_counter + 1].rel = Create_Relation(md, wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file2_ID].file1_ID,wl_ptr -> predicates[i].tuples[j].file2_column);
                    qt_predicates[predicate_counter + 1].rel = Radix_Sort(qt_predicates[predicate_counter + 1].rel);


                }



                if(qt_predicates[predicate_counter].rel != NULL && qt_predicates[predicate_counter + 1].rel != NULL)
                {

                    // Check 1st predicate for self join or scan
                    if(!Join_v2(interv_final, qt_predicates[predicate_counter].rel, qt_predicates[predicate_counter + 1].rel, qt_predicates[predicate_counter].file1_ID, qt_predicates[predicate_counter + 1].file1_ID))
                    {
                        printf("None Results\n" );
                    }
                    else
                    {

                        for (uint64_t z = 0; z < interv_final -> position; z++)
                        {
                            for(size_t i = 0; i < qt_predicates[z].rel -> num_tuples; i++)
                            {
                                free(qt_predicates[z].rel->tuples[i].payload);
                            }
                            free(qt_predicates[z].rel -> tuples);
                            free(qt_predicates[z].rel);
                            qt_predicates[z].rel = Update_Predicates(interv_final -> final_rel, z);

                        }
                    }
                    predicate_counter+=2;
                }
                else
                {
                    null_flag = 1;
                    break;
                }


            }

            else        // Rest predicates
            {


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
                    Update_Relation_Keys(md,wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID,  wl_ptr -> predicates[i].tuples[j].file1_column,qt_filters[posL].rel,0);
                    qt_predicates[predicate_counter].file1_ID = wl_ptr -> predicates[i].tuples[j].file1_ID;
                    qt_predicates[predicate_counter].file1_column = wl_ptr -> predicates[i].tuples[j].file1_column;
                    qt_predicates[predicate_counter].rel = Update_Predicates(qt_filters[posL].rel, 0);//Init_pointer();
                    // qt_predicates[predicate_counter].rel = qt_filters[posL].rel;
                    qt_predicates[predicate_counter].rel = Radix_Sort(qt_predicates[predicate_counter].rel);

                    qt_filters[posL].used = 0;
                    posL = predicate_counter;

                    predicate_counter++;
                    Lflag = 1;
                }
                else // if you don't find you have to create it
                {
                    // Check Lpart of predicate
                    if((posL = Find_Query_Tuple_Predicate(qt_predicates, wl_ptr -> predicates[i].tuples[j].file1_ID, predicate_counter)) != TAG)
                    {

                        Update_Relation_Keys(md,wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID,  wl_ptr -> predicates[i].tuples[j].file1_column,qt_predicates[posL].rel,0);
                        qt_predicates[posL].file1_ID = wl_ptr -> predicates[i].tuples[j].file1_ID;

                        qt_predicates[posL].file1_column = wl_ptr -> predicates[i].tuples[j].file1_column;
                        // printf("Lpart)  I have found it %lu.%lu-> %lu\n",qt_predicates[posL].file1_ID, qt_predicates[posL].file1_column,posL);
                        qt_predicates[posL].rel = Radix_Sort(qt_predicates[posL].rel);
                        Lflag = 2;
                        Lcolumn_tmp = qt_predicates[posL].file1_column;

                    }
                    else
                    {
                        if((qt_predicates = (query_tuple *)realloc(qt_predicates,sizeof(query_tuple) * (predicate_counter + 1))) == NULL)
                        {
                            perror("Execute_Queries 1st realloc");
                            exit(-1);
                        }
                        qt_predicates[predicate_counter].file1_ID = wl_ptr -> predicates[i].tuples[j].file1_ID;
                        qt_predicates[predicate_counter].file1_column = wl_ptr -> predicates[i].tuples[j].file1_column;
                        qt_predicates[predicate_counter].rel = Create_Relation(md,wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID,qt_predicates[predicate_counter].file1_column);
                        qt_predicates[predicate_counter].rel = Radix_Sort(qt_predicates[predicate_counter].rel);
                        posL = predicate_counter;
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

                    Update_Relation_Keys(md,wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file2_ID].file1_ID,  wl_ptr -> predicates[i].tuples[j].file2_column,qt_filters[posR].rel,0);
                    qt_predicates[predicate_counter].file1_ID = wl_ptr -> predicates[i].tuples[j].file2_ID;
                    qt_predicates[predicate_counter].file1_column = wl_ptr -> predicates[i].tuples[j].file2_column;
                    qt_predicates[predicate_counter].rel = Update_Predicates(qt_filters[posR].rel, 0);
                    qt_predicates[predicate_counter].rel = Radix_Sort(qt_predicates[predicate_counter].rel);

                    qt_filters[posR].used = 0;
                    posR = predicate_counter;

                    predicate_counter++;
                    Rflag = 1;

                }
                else // if you don't find you have to create it
                {
                    // Check Rpart of predicate
                    if((posR = Find_Query_Tuple_Predicate(qt_predicates, wl_ptr -> predicates[i].tuples[j].file2_ID, predicate_counter)) != TAG)
                    {

                        Update_Relation_Keys(md,wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file2_ID].file1_ID,  wl_ptr -> predicates[i].tuples[j].file2_column,qt_predicates[posR].rel,0);
                        qt_predicates[posR].file1_ID = wl_ptr -> predicates[i].tuples[j].file2_ID;
                        qt_predicates[posR].file1_column = wl_ptr -> predicates[i].tuples[j].file2_column;
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
                        qt_predicates[predicate_counter].file1_ID = wl_ptr -> predicates[i].tuples[j].file2_ID;
                        qt_predicates[predicate_counter].file1_column = wl_ptr -> predicates[i].tuples[j].file2_column;
                        qt_predicates[predicate_counter].rel = Create_Relation(md,wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file2_ID].file1_ID,qt_predicates[predicate_counter].file1_column);
                        qt_predicates[predicate_counter].rel = Radix_Sort(qt_predicates[predicate_counter].rel);
                        posR = predicate_counter;
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
                     // printf("Case: 1,2!!\n");
                    pos = FindRowID(interv_final, qt_predicates[posR].file1_ID);
                    // printf("aaa\n");
                    // printf("Eimaste edww ---> %lu\n",pos);
                    // printf("%lu.%lu\n",wl_ptr -> predicates[i].tuples[j].file2_ID, wl_ptr -> predicates[i].tuples[j].file2_column);
                    Update_Relation_Keys(md, wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file2_ID].file1_ID, wl_ptr -> predicates[i].tuples[j].file2_column, interv_final -> final_rel, pos);
                    // printf("aaa\n");
                    interv_final -> final_rel = Radix_Sort(interv_final -> final_rel);
                    // uint64_t posFilter = Find_Query_Tuple(qt_filters, qt_filters[posL].file1_ID, filter_counter);
                    // printf("%lu.%lu\n",qt_filters[posL].file1_ID,qt_filters[posL].file1_column);
                    // printf("%lu.%lu=%lu.%lu\n",qt_predicates[posL].file1_ID,qt_predicates[posL].file1_column,qt_predicates[posR].file1_ID,qt_predicates[posR].file1_column);
                    // printf("posL = %lu - %lu.%lu\n",posL,qt_predicates[posL].file1_ID, qt_predicates[posL].file1_column);
                    // printf("posR = %lu - %lu.%lu\n",posR,qt_predicates[posR].file1_ID, qt_predicates[posR].file1_column);

                    // Print_Relation_2(qt_filters[2].rel);
                    // qt_filters[posL].rel = Update_Predicates(qt_predicates, posR);
                    // Update_Relation_Keys(md, wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID, wl_ptr -> predicates[i].tuples[j].file1_column, qt_filters[posL].rel, 0);
                    // printf("posL = %lu - %lu.%lu\n",posL,qt_filters[posL].file1_ID, qt_filters[posL].file1_column);
                    // printf("posL = %lu - %lu.%lu\n",posL, wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID, wl_ptr -> predicates[i].tuples[j].file1_column);

                    if(!Join_v2(interv_final, interv_final -> final_rel, qt_predicates[posL].rel, qt_predicates[posL].file1_ID, qt_predicates[posR].file1_ID))
                    {
                        // Join_v2(interv_final , interv_final -> final_rel, qt_predicates[predicate_counter + 1].rel, qt_predicates[predicate_counter].file1_ID, qt_predicates[predicate_counter + 1].file1_ID);
                        // printf("None Results\n" );
                        null_flag_Join = 1;
                        break;
                    }
                    else
                    {

                    }
                    // exit(-1);
                }
                else if(Lflag == 1 && Rflag == 3)
                {
                    printf("Case: 1,3!!\n");
                    exit(-1);
                }
                if(Lflag == 2 && Rflag == 1) //  intervening is going to take Lpart
                {
                    // Print_Available_Filters(qt_filters, filter_counter);

                    // printf("Case 2 1\n");
                    pos = FindRowID(interv_final, qt_predicates[posL].file1_ID);

                    Update_Relation_Keys(md, wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID, wl_ptr -> predicates[i].tuples[j].file1_column, interv_final -> final_rel, pos);
                    interv_final -> final_rel = Radix_Sort(interv_final -> final_rel);


                    if(!Join_v2(interv_final, interv_final -> final_rel, qt_predicates[posR].rel, qt_predicates[posL].file1_ID, qt_predicates[posR].file1_ID))
                    {
                        // printf("None Results ,2,1\n" );
                        null_flag_Join = 1;
                        break;
                    }
                    else
                    {

                        // for (uint64_t z = 0; z < interv_final -> position; z++)
                        // {
                        //     qt_predicates[z].rel = Update_Predicates(interv_final -> final_rel, z);
                        // //    Print_Relation_2(qt_predicates[z].rel);
                        // }

                    }

                }
                else if(Lflag == 2 && Rflag == 2)    // SELF JOIN
                {
                    // printf("2.2\n");
                    if(wl_ptr -> predicates[i].tuples[j].file1_ID == wl_ptr -> predicates[i].tuples[j].file2_ID)
                    {
                        if(!Scan(interv_final, interv_final -> final_rel, qt_predicates[posR].rel, qt_predicates[posL].file1_ID, qt_predicates[posR].file1_ID))
                        {
                            printf("None Results->2,2\n" );
                            null_flag_Join = 1;
                            break;
                        }
                        else
                        {
                            Print_Relation_2(interv_final -> final_rel);

                        }
                    }
                    else
                    {
                        // Print_Relation_2(interv_final -> final_rel);


                        if(wl_ptr -> parameters[i].tuples[wl_ptr-> predicates[i].tuples[j].file1_ID].file1_ID == wl_ptr ->parameters[i].tuples[wl_ptr-> predicates[i].tuples[j].file2_ID].file1_ID)
                        {

                            pos = FindRowID(interv_final, qt_predicates[posL].file1_ID);
                            Update_Relation_Keys(md, qt_predicates[posL].file1_ID, Lcolumn_tmp, interv_final -> final_rel, pos);
                            interv_final -> final_rel = Radix_Sort(interv_final -> final_rel);

                            for(size_t k = 0; k < qt_predicates[posR].rel -> num_tuples; k++)
                            {
                                free(qt_predicates[posR].rel->tuples[k].payload);
                            }
                            free(qt_predicates[posR].rel -> tuples);
                            free(qt_predicates[posR].rel);
                            qt_predicates[posR].rel = Update_Predicates(interv_final -> final_rel, pos);
                            Update_Relation_Keys(md, qt_predicates[posR].file1_ID, qt_predicates[posR].file1_column, qt_predicates[posR].rel, 0);
                            qt_predicates[posR].rel = Radix_Sort(qt_predicates[posR].rel);

                            if(!Join_v2(interv_final, interv_final -> final_rel, qt_predicates[posR].rel, qt_predicates[posL].file1_ID, qt_predicates[posR].file1_ID))
                            {
                                printf("None Results, 2,2 else\n" );
                                null_flag_Join = 1;
                                break;
                            }
                            else
                            {

                            }
                        }
                        else
                        {
                            // printf("%lu)edw\n",i);

                            pos = FindRowID(interv_final, qt_predicates[posL].file1_ID);
                            Update_Relation_Keys(md, wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID, wl_ptr -> predicates[i].tuples[j].file1_column, interv_final -> final_rel, pos);
                            for(size_t k = 0; k < qt_predicates[posR].rel -> num_tuples; k++)
                            {
                                free(qt_predicates[posR].rel->tuples[k].payload);
                            }
                            free(qt_predicates[posR].rel -> tuples);
                            free(qt_predicates[posR].rel);
                            qt_predicates[posR].rel = Update_Predicates(interv_final -> final_rel, posR);
                            Update_Relation_Keys(md, wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file2_ID].file1_ID, wl_ptr -> predicates[i].tuples[j].file2_column, qt_predicates[posR].rel, 0);
                            if(!Scan(interv_final, interv_final -> final_rel, qt_predicates[posR].rel, qt_predicates[posL].file1_ID, qt_predicates[posR].file1_ID))
                            {
                                printf("None Results 2,2 else else\n" );
                                exit(-1);
                            }
                            else
                            {
                                // Print_Relation_2(interv_final -> final_rel);


                            }
                        }


                    }
                }
                //  rowid 3|0
                // 3.1 = 1.0
                else if(Lflag == 2 && Rflag == 3)  //   intervening is going to take Lpart
                {
                    // printf("%lu\n",interv_final -> final_rel ->num_tuples);
                    //
                    // printf("%lu\n",qt_predicates[posR].rel ->num_tuples);
                    // printf("Eimaste edww ---> %lu\n",posL );
                    pos = FindRowID(interv_final, qt_predicates[posL].file1_ID);

                    // printf("Eimaste edww ---> %lu\n",pos);
                    Update_Relation_Keys(md, wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file1_ID].file1_ID, wl_ptr -> predicates[i].tuples[j].file1_column, interv_final -> final_rel, pos);

                    interv_final -> final_rel = Radix_Sort(interv_final -> final_rel);

                    // printf("%lu.%lu=%lu.%lu\n",qt_predicates[posL].file1_ID,qt_predicates[posL].file1_column,qt_predicates[posR].file1_ID,qt_predicates[posR].file1_column);
                    if(!Join_v2(interv_final, interv_final -> final_rel, qt_predicates[posR].rel, qt_predicates[posL].file1_ID, qt_predicates[posR].file1_ID))
                    {
                        // Join_v2(interv_final, interv_final -> final_rel, qt_predicates[predicate_counter + 1].rel, qt_predicates[predicate_counter].file1_ID, qt_predicates[predicate_counter + 1].file1_ID);
                        // printf("None Results\n" );
                        null_flag_Join = 1;
                        break;
                    }
                    else
                    {
                        //    Print_Relation_2(interv_final -> final_rel);
                        // printf("%lu\n",interv_final -> final_rel ->num_tuples);
                        //qt_predicates[posR].rel = Update_Predicates(interv_final -> final_rel, pos);
                        //    Print_Relation_2(qt_predicates[posR].rel);

                        // for (uint64_t z = 0; z < interv_final -> position; z++)
                        // {
                        //     qt_predicates[z].rel = Update_Predicates(interv_final -> final_rel, z);
                        // //    Print_Relation_2(qt_predicates[z].rel);
                        // }

                    }
                    // printf("%lu\n",qt_predicates[posR].rel ->num_tuples);
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
                    // printf("case 3,2 \n");
                    pos = FindRowID(interv_final, qt_predicates[posR].file1_ID);

                    // printf("Eimaste edww ---> %lu\n",pos);
                    Update_Relation_Keys(md, wl_ptr -> parameters[i].tuples[wl_ptr -> predicates[i].tuples[j].file2_ID].file1_ID, wl_ptr -> predicates[i].tuples[j].file2_column, interv_final -> final_rel, pos);

                    interv_final -> final_rel = Radix_Sort(interv_final -> final_rel);

                    // printf("%lu.%lu=%lu.%lu\n",qt_predicates[posL].file1_ID,qt_predicates[posL].file1_column,qt_predicates[posR].file1_ID,qt_predicates[posR].file1_column);
                    if(!Join_v2(interv_final, interv_final -> final_rel, qt_predicates[posR].rel, qt_predicates[posL].file1_ID, qt_predicates[posR].file1_ID))
                    {
                        null_flag_Join = 1;
                        break;
                    }
                    else
                    {


                    }
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

         printf("%lu)",i);

    //    exit(-1);
        if(null_flag == 1)
        {
            // printf("%lu)",i);
            for (uint64_t z = 0; z < wl_ptr -> selects[i].num_tuples; z++) // for every select
            {

                printf("NULL");
                if(z + 1 ==  wl_ptr -> selects[i].num_tuples)
                break;
                else
                printf(" ");


            }
            printf("\n");
            for (uint64_t z = 0; z < filter_counter; z++)
            {
                free(qt_filters[z].rel);
            }
            free(qt_filters);
            free(interv_final);
            if(predicate_counter > 0)
            free(qt_predicates);

            return;
        }
        else if(null_flag_Join == 1)
        {
            // printf("%lu)",i);
            for (uint64_t z = 0; z < wl_ptr -> selects[i].num_tuples; z++) // for every select
            {
                printf("NULL");
                if(z + 1 ==  wl_ptr -> selects[i].num_tuples)
                break;
                else
                printf(" ");


            }
            printf("\n");
            for (uint64_t z = 0; z < filter_counter; z++)
            {
                for (size_t k = 0; k < qt_filters[z].rel -> num_tuples; k++)
                {
                    free(qt_filters[z].rel -> tuples[k].payload);
                }
                free(qt_filters[z].rel -> tuples);
                free(qt_filters[z].rel);
            }
            free(qt_filters);
            //
            //
            //
            for (uint64_t z = 0; z < predicate_counter; z++)
            {
                for (size_t k = 0; k < qt_predicates[z].rel -> num_tuples; k++)
                {
                    free(qt_predicates[z].rel -> tuples[k].payload);
                }
                free(qt_predicates[z].rel -> tuples);
                free(qt_predicates[z].rel);
            }
            free(qt_predicates);
            //
            // // if()
            // for (size_t z = 0; z < interv_final -> final_rel -> num_tuples; z++)
            // {
            //     free(interv_final -> final_rel ->tuples[z].payload);
            // }
            // free(interv_final -> final_rel ->tuples);
            // free(interv_final -> final_rel);
            // // //
            free(interv_final -> rowId);
            free(interv_final);


            return;
        }
        else
        {
            for (uint64_t z = 0; z < wl_ptr -> selects[i].num_tuples; z++) // for every select
            {
                for (uint64_t x = 0; x < interv_final -> position; x++)   // Search in interval final in rowid
                {

                    if(interv_final -> rowId[x] == wl_ptr -> selects[i].tuples[z].file1_ID)
                    {

                        CheckSum(md,wl_ptr -> parameters[i].tuples[wl_ptr -> selects[i].tuples[z].file1_ID].file1_ID,wl_ptr -> selects[i].tuples[z].file1_column,interv_final -> final_rel,x);
                        if(z + 1 ==  wl_ptr -> selects[i].num_tuples)
                        break;
                        else
                        printf(" ");
                        break;
                    }
                }
            }
        }
        printf("\n");
        // printf("TUPLEs : %lu \n", interv_final -> final_rel ->num_tuples);
        // Print_Relation_2(interv_final -> final_rel);

        // Print_Available_Filters(qt_filters, filter_counter);
        for (uint64_t z = 0; z < filter_counter; z++)
        {
            for (size_t k = 0; k < qt_filters[z].rel -> num_tuples; k++)
            {
                free(qt_filters[z].rel -> tuples[k].payload);
            }
            free(qt_filters[z].rel -> tuples);
            free(qt_filters[z].rel);
        }
        free(qt_filters);



        for (uint64_t z = 0; z < predicate_counter; z++)
        {
            for (size_t k = 0; k < qt_predicates[z].rel -> num_tuples; k++)
            {
                free(qt_predicates[z].rel -> tuples[k].payload);
            }
            free(qt_predicates[z].rel -> tuples);
            free(qt_predicates[z].rel);
        }
        free(qt_predicates);

        // if()
        for (size_t z = 0; z < interv_final -> final_rel -> num_tuples; z++)
        {
            free(interv_final -> final_rel ->tuples[z].payload);
        }
        free(interv_final -> final_rel ->tuples);
        free(interv_final -> final_rel);
        // //
        free(interv_final -> rowId);
        free(interv_final);


        filter_counter = 0;
        predicate_counter = 0;
        free(bt);
        // printf("-------------------------------------------------------------------\n",i);
    }


}

uint64_t Current_Best_Predicate(uint64_t num_predicates, uint64_t pred, uint64_t j)
{
    if (num_predicates == 3)
    {
        if(pred == 0)
        {
            j /= 100;
        }
        else if(pred == 1)
        {
            j /= 10;
            j %= 10;
        }
        else
        {
            j %= 10;
        }
    }
    else if(num_predicates == 2)
    {
        if(pred == 0)
        {
            j /= 10;
        }
        else if(pred == 1)
        {
            j %= 10;
        }
    }
    else
    {
        j %= 10;
    }
    return j;
}
