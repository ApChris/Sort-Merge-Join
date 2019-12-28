#include"../include/statistics.h"

statistics * Calculate_Statistics(metadata * md, uint64_t rows)
{
    statistics * stats;



    uint64_t offset_metadata = 2;

    if ((stats = (statistics *)malloc(rows*sizeof(statistics))) == NULL)
    {
        perror("Read_binary_file statistics malloc failed:");
        exit(-1);
    }

    // for every binary file
    for (size_t i = 0; i < rows; i++)
    {


            /* ------------------------------------- PART 3 FOR STATISTICS ------------------------------------- */

            // # values of row i
            if ((stats[i].fa = (uint64_t *)malloc((md[i].num_columns*sizeof(uint64_t)))) == NULL)
            {
                perror("stats[i].fa malloc failed:");
                exit(-1);
            }

            // min value of row i
            if ((stats[i].Ia = (uint64_t *)malloc((md[i].num_columns*sizeof(uint64_t)))) == NULL)
            {
                perror("stats[i].Ia malloc failed:");
                exit(-1);
            }
            // max value of row i
            if ((stats[i].ua = (uint64_t *)malloc((md[i].num_columns*sizeof(uint64_t)))) == NULL)
            {
                perror("stats[i].ua malloc failed:");
                exit(-1);
            }

            uint64_t min_value, max_value;
            uint64_t current_value;

            printf("---------------r%lu---------------\n", i);
            for (size_t k = 0; k < md[i].num_columns; k++)
            {
                min_value = md[i].full_array[(k*md[i].num_tuples)+2];
                max_value = min_value;
                for (size_t w = 0; w < md[i].num_tuples; w++)
                {
                    current_value = md[i].full_array[(k*md[i].num_tuples)+2+w];
                    if(min_value > current_value)
                    {
                        min_value = current_value;
                    }
                    if(max_value < current_value)
                    {
                        max_value = current_value;
                    }
                }
                stats[i].Ia[k] = min_value;
                stats[i].ua[k] = max_value;
                stats[i].fa[k] = md[i].num_tuples;
                printf("r%lu|%lu\ttotal values:%lu\tmin:%lu\tmax:%lu\n", i, k, stats[i].fa[k], stats[i].Ia[k], stats[i].ua[k]);
            }
            // printf("\n");

            // for every i create arrays of distinct(boolean) arrays
            if ((stats[i].da = (bool **)malloc((md[i].num_columns*sizeof(bool *)))) == NULL)
            {
                perror("stats[i].da malloc failed:");
                exit(-1);
            }

            // i-th number of distinct values
            if ((stats[i].num_da = (uint64_t *)malloc((md[i].num_columns*sizeof(uint64_t)))) == NULL)
            {
                perror("stats[i].num_da malloc failed:");
                exit(-1);
            }
            // the size of every distinct value array (we need to know the exact number of distinct values in order to free them)
            if ((stats[i].size_da = (uint64_t *)malloc((md[i].num_columns*sizeof(uint64_t)))) == NULL)
            {
                perror("stats[i].size_da malloc failed:");
                exit(-1);
            }

            printf("%lu---->\n",md[i].num_columns);
            // for every column of i create distinct(boolean) arrays
            uint64_t size = N;
            for (uint64_t k = 0; k < md[i].num_columns; k++)
            {
                // if ua - Ia + 1 > N, then allocate N bytes
                if ((stats[i].ua[k] - stats[i].Ia[k] + 1) < N)
                {
                    size = stats[i].ua[k] - stats[i].Ia[k] + 1;
                }
                if ((stats[i].da[k] = (bool *)malloc(size*sizeof(bool))) == NULL)
                {
                    perror("bool malloc failed:");
                    exit(-1);
                }
                stats[i].size_da[k] = size;
                // init bool array to false
                for (size_t w = 0; w < stats[i].size_da[k]; w++)
                {
                    stats[i].da[k][w] = false;
                }
            }

            for (size_t k = 0; k < md[i].num_columns; k++)
            {
                stats[i].num_da[k] = 0;
                for (size_t w = 0; w < md[i].num_tuples; w++)
                {
                    current_value = md[i].full_array[(k*md[i].num_tuples)+2+w];
                    // make current cell true: current_value - Ia % N(size)
                    if (stats[i].da[k][(current_value - stats[i].Ia[k]) % stats[i].size_da[k]] == false)
                    {
                        stats[i].da[k][(current_value - stats[i].Ia[k]) % stats[i].size_da[k]] = true;
                        stats[i].num_da[k]++;
                    }
                }
                printf("r%lu|%lu\tdistinct values:%lu\n", i, k, stats[i].num_da[k]);
            }

            /* ------------------------------------- END OF STATISTICS ------------------------------------- */



    }
    return stats;
}
