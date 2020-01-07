#include "../include/bestTree.h"


// void Deallocate_BestTree(best_tree_node **best_tree,batch_listnode *curr_query,relation_map* rel_map)
// {
//   int size = pow(2, curr_query->num_of_relations);
//   for (size_t i = 1; i < size; i++)
//   {
//     if (best_tree[i]->best_tree != NULL)
//     	FreePredicateList(best_tree[i]->best_tree);
//     if(best_tree[i]->tree_stats != NULL)
//       FreeQueryStats(best_tree[i]->tree_stats,curr_query,rel_map);
//     free(best_tree[i]);
//   }
//   free(best_tree);
// }
//
// uint64_t Create_JoinTree(best_tree ** final_bt, best_tree * bt, query_tuple * qt_query_tree, uint64_t qt_columns, work_line * wl_ptr, uint64_t s_new_num)
// {
//     uint64_t num = s_new_num;
//     uint64_t bit_num = 0;
//
//     (*final_bt) = malloc(sizeof(best_tree));
//     (*final_bt) -> qt_query_tree = NULL;
//
//     while(num != 0)
//     {
//         if (num % 2 == 1)
//         {
//             bit_num++;
//         }
//       num = num >> 1;
//     }
//     (*final_bt) -> active_bits = bit_num;
//     (*final_bt) -> num_predicates = 0;
//     (*final_bt) -> tree_stats = calloc(qt_columns, sizeof(statistics *));
//
//
//
// }
bestTree * Best_Tree_Init()
{
    bestTree * bt;
    if ((bt = ((bestTree *)malloc(sizeof(bestTree)))) == NULL)
    {
        perror("JoinEnum first malloc");
        exit(-1);
    }
    bt -> cost = 0;
    bt -> path = 0;

    return bt;
}

void Best_Tree_Update(bestTree * bt, uint64_t path, double cost)
{
    bt -> path = path;
    bt -> cost = cost;
}
bestTree * Join_Enum(statistics * query_stats, work_line * wl_ptr, uint64_t pos)
{

    // printf("------------------------JOIN ENUM----------------------!\n");
    // bestTree *
    // printf("%lu %lu %lu\n",wl_ptr->parameters[pos].tuples[0], wl_ptr->parameters[pos].tuples[1] , wl_ptr->parameters[pos].tuples[2]);
    uint64_t num_predicates = wl_ptr -> predicates[pos].num_tuples;
    // printf("%lu\n",num_predicates);
    bestTree * bt;

    bt = Best_Tree_Init();

    uint64_t best_path = 0;
    double cost = 0;
    double best_cost = 0;
    double array_cost[num_predicates - 1];
    bool first_Time = true;
    // for all predicates
    for (size_t i = 0; i < num_predicates; i++)
    {

        if(num_predicates > 1)
        {
            // for rest of predicates
            for (size_t j = 0; j < num_predicates; j++)
            {

                // cost = Calculate_Cost(query_stats, i,  j, cost);
                // skip the same predicate
                if(i == j)
                {
                    continue;
                }
                if(num_predicates == 3)
                {
                    for (size_t k = 0; k < num_predicates; k++)
                    {
                        if(k == i || k == j)
                        {
                            continue;
                        }
                        cost = 0;
                        // printf("------------------\n%lu.%lu = %lu.%lu&",wl_ptr -> predicates[pos].tuples[i].file1_ID, wl_ptr -> predicates[pos].tuples[i].file1_column, wl_ptr -> predicates[pos].tuples[i].file2_ID, wl_ptr -> predicates[pos].tuples[i].file2_column);
                        // printf("%lu.%lu = %lu.%lu&",wl_ptr -> predicates[pos].tuples[j].file1_ID, wl_ptr -> predicates[pos].tuples[j].file1_column, wl_ptr -> predicates[pos].tuples[j].file2_ID, wl_ptr -> predicates[pos].tuples[j].file2_column);
                        // printf("%lu.%lu = %lu.%lu\n\n",wl_ptr -> predicates[pos].tuples[k].file1_ID, wl_ptr -> predicates[pos].tuples[k].file1_column, wl_ptr -> predicates[pos].tuples[k].file2_ID, wl_ptr -> predicates[pos].tuples[k].file2_column);
                        cost = Calculate_Cost(query_stats, wl_ptr -> predicates[pos].tuples[i].file1_ID, wl_ptr -> predicates[pos].tuples[i].file1_column, wl_ptr -> predicates[pos].tuples[i].file2_ID, wl_ptr -> predicates[pos].tuples[i].file2_column, cost);
                        array_cost[0] = cost;
                        cost = Calculate_Cost(query_stats, wl_ptr -> predicates[pos].tuples[j].file1_ID, wl_ptr -> predicates[pos].tuples[j].file1_column, wl_ptr -> predicates[pos].tuples[j].file2_ID, wl_ptr -> predicates[pos].tuples[j].file2_column, cost);
                        array_cost[1] = cost;
                        if(first_Time)
                        {
                            best_cost = array_cost[0] + array_cost[1];
                            best_path = 100*i + 10*j + 1*k;
                            first_Time = false;
                        }
                        if(best_cost > array_cost[0] + array_cost[1])
                        {
                            best_cost = array_cost[0] + array_cost[1];
                            best_path = 100*i + 10*j + 1*k;


                        }
                        cost = Calculate_Cost(query_stats, wl_ptr -> predicates[pos].tuples[k].file1_ID, wl_ptr -> predicates[pos].tuples[k].file1_column, wl_ptr -> predicates[pos].tuples[k].file2_ID, wl_ptr -> predicates[pos].tuples[k].file2_column, cost);


                        // printf("\nCombination: %lu %lu %lu\nTOTAL COST : %lf\n-----------------------------\n\n",i,j,k, cost);
                        // cost = Calculate_Cost(query_stats, k,  k, cost);
                    }
                }
                else if(num_predicates  == 4)
                {
                    printf("NUM PREDICATES = 4 FIX IT\n");
                    exit(-1);
                }
                // predicates == 2
                else
                {
                    cost = 0;
                    // printf("------------------\n%lu.%lu = %lu.%lu&",wl_ptr -> predicates[pos].tuples[i].file1_ID, wl_ptr -> predicates[pos].tuples[i].file1_column, wl_ptr -> predicates[pos].tuples[i].file2_ID, wl_ptr -> predicates[pos].tuples[i].file2_column);
                    // printf("%lu.%lu = %lu.%lu\n",wl_ptr -> predicates[pos].tuples[j].file1_ID, wl_ptr -> predicates[pos].tuples[j].file1_column, wl_ptr -> predicates[pos].tuples[j].file2_ID, wl_ptr -> predicates[pos].tuples[j].file2_column);
                    cost = Calculate_Cost(query_stats, wl_ptr -> predicates[pos].tuples[i].file1_ID, wl_ptr -> predicates[pos].tuples[i].file1_column, wl_ptr -> predicates[pos].tuples[i].file2_ID, wl_ptr -> predicates[pos].tuples[i].file2_column, cost);
                    array_cost[0] = cost;
                    cost = Calculate_Cost(query_stats, wl_ptr -> predicates[pos].tuples[j].file1_ID, wl_ptr -> predicates[pos].tuples[j].file1_column, wl_ptr -> predicates[pos].tuples[j].file2_ID, wl_ptr -> predicates[pos].tuples[j].file2_column, cost);
                    if(first_Time)
                    {
                        best_cost = array_cost[0];
                        best_path = 10*i + 1*j;
                        first_Time = false;
                    }
                    if(best_cost > array_cost[0])
                    {
                        best_cost = array_cost[0];
                        best_path = 10*i + 1*j;

                    }
                    // printf("\nCombination: %lu %lu\nTOTAL COST : %lf\n-----------------------------\n\n",i,j,cost);
                }

            }
        }
        // if we have only 1 predicate
        else
        {

        }

    }
    Best_Tree_Update(bt , best_path,best_cost);
    // printf("Best cost is %lf\nBest path is %lu\n",best_cost,best_path);
    // printf("Best cost is %lf\nBest path is %lu\n",bt -> cost,bt -> path);
    // printf("-------------------------------------------------------!\n");
    return bt;
}

double Calculate_Cost(statistics * query_stats, uint64_t file1_ID, uint64_t file1_column, uint64_t file2_ID, uint64_t file2_column, double cost)
{
    // printf("%lu.%lu -> fa %lf\t",file1_ID, file1_column, query_stats[file1_ID].fa[file1_column]);
    // printf("%lu.%lu -> fa %lf\t",file2_ID, file2_column, query_stats[file2_ID].fa[file2_column]);
    cost += query_stats[file1_ID].fa[file1_column] + query_stats[file2_ID].fa[file2_column];
    // printf("Current cost %lf\n",cost);

    return cost;
}
