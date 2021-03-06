#include"../include/statistics.h"
statistics * Calculate_Statistics(metadata * md, uint64_t rows)
{
    statistics * stats;

    if ((stats = (statistics *)malloc(rows*sizeof(statistics))) == NULL)
    {
        perror("Read_binary_file statistics malloc failed:");
        exit(-1);
    }

    // for every binary file
    for (size_t i = 0; i < rows; i++)
    {

            // # values of row i
            if ((stats[i].fa = (double *)malloc((md[i].num_columns*sizeof(double)))) == NULL)
            {
                perror("stats[i].fa malloc failed:");
                exit(-1);
            }

            // min value of row i
            if ((stats[i].la = (uint64_t *)malloc((md[i].num_columns*sizeof(uint64_t)))) == NULL)
            {
                perror("stats[i].la malloc failed:");
                exit(-1);
            }
            // max value of row i
            if ((stats[i].ua = (uint64_t *)malloc((md[i].num_columns*sizeof(uint64_t)))) == NULL)
            {
                perror("stats[i].ua malloc failed:");
                exit(-1);
            }
            stats[i].num_columns = md[i].num_columns;
            uint64_t min_value, max_value;
            uint64_t current_value;

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
                stats[i].la[k] = min_value;
                stats[i].ua[k] = max_value;
                stats[i].fa[k] = md[i].num_tuples;
            }
            bool **da;
            // for every i create arrays of distinct(boolean) arrays
            if ((da = (bool **)malloc((md[i].num_columns*sizeof(bool *)))) == NULL)
            {
                perror("stats[i].da malloc failed:");
                exit(-1);
            }

            // i-th number of distinct values
            if ((stats[i].da = (double *)malloc((md[i].num_columns*sizeof(double)))) == NULL)
            {
                perror("stats[i].da malloc failed:");
                exit(-1);
            }
            // the size of every distinct value array (we need to know the exact number of distinct values in order to free them)
            if ((stats[i].size_da = (uint64_t *)malloc((md[i].num_columns*sizeof(uint64_t)))) == NULL)
            {
                perror("stats[i].size_da malloc failed:");
                exit(-1);
            }

            // for every column of i create distinct(boolean) arrays
            uint64_t size = N;
            for (uint64_t k = 0; k < md[i].num_columns; k++)
            {
                // if ua - la + 1 > N, then allocate N bytes
                if ((stats[i].ua[k] - stats[i].la[k] + 1) < N)
                {
                    size = stats[i].ua[k] - stats[i].la[k] + 1;
                }
                if ((da[k] = (bool *)malloc(size*sizeof(bool))) == NULL)
                {
                    perror("bool malloc failed:");
                    exit(-1);
                }
                stats[i].size_da[k] = size;
                // init bool array to false
                for (size_t w = 0; w < stats[i].size_da[k]; w++)
                {
                    da[k][w] = false;
                }
            }

            for (size_t k = 0; k < md[i].num_columns; k++)
            {
                stats[i].da[k] = 0;
                for (size_t w = 0; w < md[i].num_tuples; w++)
                {
                    current_value = md[i].full_array[(k*md[i].num_tuples)+2+w];
                    // make current cell true: current_value - la % N(size)
                    if (da[k][(current_value - stats[i].la[k]) % stats[i].size_da[k]] == false)
                    {
                        da[k][(current_value - stats[i].la[k]) % stats[i].size_da[k]] = true;
                        stats[i].da[k]++;
                    }
                }
            }

            for (size_t j = 0; j < md[i].num_columns; j++)
            {
                free(da[j]);
            }
            free(da);


    }
    return stats;
}




statistics * Init_Query_Stats(statistics * stats, work_line * wl_ptr, uint64_t pos)
{
    statistics * query_stats;
    if((query_stats = (statistics *)malloc(sizeof(statistics) * wl_ptr -> parameters[pos].num_tuples)) == NULL)
    {
        perror("InitQuery_Stats error");
        exit(-1);
    }

    for (size_t i = 0; i < wl_ptr -> parameters[pos].num_tuples; i++)
    {

        // min value of row i
        if ((query_stats[i].la = (uint64_t *)malloc((stats[wl_ptr -> parameters[pos].tuples[i].file1_ID].num_columns*sizeof(uint64_t)))) == NULL)
        {
            perror("query_stats[i].la malloc failed:");
            exit(-1);
        }
        // max value of row i
        if ((query_stats[i].ua = (uint64_t *)malloc((stats[wl_ptr -> parameters[pos].tuples[i].file1_ID].num_columns*sizeof(uint64_t)))) == NULL)
        {
            perror("query_stats[i].ua malloc failed:");
            exit(-1);
        }

        if ((query_stats[i].fa = (double *)malloc((stats[wl_ptr -> parameters[pos].tuples[i].file1_ID].num_columns*sizeof(double)))) == NULL)
        {
            perror("query_stats[i].fa malloc failed:");
            exit(-1);
        }

        if ((query_stats[i].da = (double *)malloc((stats[wl_ptr -> parameters[pos].tuples[i].file1_ID].num_columns*sizeof(double)))) == NULL)
        {
            perror("query_stats[i].da malloc failed:");
            exit(-1);
        }
        // the size of every distinct value array (we need to know the exact number of distinct values in order to free them)
        if ((query_stats[i].size_da = (uint64_t *)malloc((stats[wl_ptr -> parameters[pos].tuples[i].file1_ID].num_columns*sizeof(uint64_t)))) == NULL)
        {
            perror("query_stats[i].size_da malloc failed:");
            exit(-1);
        }
        for (size_t j = 0; j < stats[wl_ptr -> parameters[pos].tuples[i].file1_ID].num_columns; j++)
        {
            query_stats[i].la[j] = stats[wl_ptr -> parameters[pos].tuples[i].file1_ID].la[j];
            query_stats[i].ua[j] = stats[wl_ptr -> parameters[pos].tuples[i].file1_ID].ua[j];
            query_stats[i].fa[j] = stats[wl_ptr -> parameters[pos].tuples[i].file1_ID].fa[j];
            query_stats[i].da[j] = stats[wl_ptr -> parameters[pos].tuples[i].file1_ID].da[j];
            query_stats[i].size_da[j] = stats[wl_ptr -> parameters[pos].tuples[i].file1_ID].size_da[j];

        }
        query_stats[i].num_columns = stats[wl_ptr -> parameters[pos].tuples[i].file1_ID].num_columns;
    }

    return query_stats;

}

void Update_Query_Stats(statistics * query_stats, work_line * wl_ptr, uint64_t pos)
{
    // for all filters
    for (uint64_t i = 0; i < wl_ptr -> filters[pos].num_tuples; i++)
    {

        double old_fa_global = query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].fa[wl_ptr -> filters[pos].tuples[i].file1_column];
        if(wl_ptr -> filters[pos].tuples[i].symbol == '>' || wl_ptr -> filters[pos].tuples[i].symbol == '<')
        {
            uint64_t k1, k2;

            if(wl_ptr -> filters[pos].tuples[i].symbol == '<')
            {
                if(wl_ptr -> filters[pos].tuples[i].limit > query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].ua[wl_ptr -> filters[pos].tuples[i].file1_column])
                {
                    k2 = query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].ua[wl_ptr -> filters[pos].tuples[i].file1_column];
                }
                else
                {
                    k2 = wl_ptr -> filters[pos].tuples[i].limit;
                }
                k1 = query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].la[wl_ptr -> filters[pos].tuples[i].file1_column];
            }
            else
            {
                if(wl_ptr -> filters[pos].tuples[i].limit < query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].la[wl_ptr -> filters[pos].tuples[i].file1_column])
                {
                    k1 = query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].la[wl_ptr -> filters[pos].tuples[i].file1_column];
                }
                else
                {
                    k1 = wl_ptr -> filters[pos].tuples[i].limit;
                }
                k2 = query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].ua[wl_ptr -> filters[pos].tuples[i].file1_column];
            }

            // min & max are the same number , none distinct values
            if(query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].ua[wl_ptr -> filters[pos].tuples[i].file1_column] - query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].la[wl_ptr -> filters[pos].tuples[i].file1_column] == 0)
            {
                query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].da[wl_ptr -> filters[pos].tuples[i].file1_column] = 0;
                query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].la[wl_ptr -> filters[pos].tuples[i].file1_column] = 0;
            }
            else
            {
                uint64_t old_min = query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].la[wl_ptr -> filters[pos].tuples[i].file1_column];
                uint64_t old_max = query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].ua[wl_ptr -> filters[pos].tuples[i].file1_column];
                double old_da = query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].da[wl_ptr -> filters[pos].tuples[i].file1_column];
                double old_fa = query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].fa[wl_ptr -> filters[pos].tuples[i].file1_column];

                query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].da[wl_ptr -> filters[pos].tuples[i].file1_column] = (((double)(k2 - k1)/(old_max - old_min)) * (old_da));
                query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].fa[wl_ptr -> filters[pos].tuples[i].file1_column] = (((double)(k2 - k1)/(old_max - old_min)) * (old_fa));
            }

            query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].la[wl_ptr -> filters[pos].tuples[i].file1_column] = k1;
            query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].ua[wl_ptr -> filters[pos].tuples[i].file1_column] = k2;


        }
        else if(wl_ptr -> filters[pos].tuples[i].symbol == '=')
        {
            // if k exists in da
            // if limit >= min && limit <= max
            if(wl_ptr -> filters[pos].tuples[i].limit >= query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].la[wl_ptr -> filters[pos].tuples[i].file1_column] &&
            wl_ptr -> filters[pos].tuples[i].limit <= query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].ua[wl_ptr -> filters[pos].tuples[i].file1_column])
            {
                double old_da = query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].da[wl_ptr -> filters[pos].tuples[i].file1_column];
                double old_fa = query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].fa[wl_ptr -> filters[pos].tuples[i].file1_column];

                query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].da[wl_ptr -> filters[pos].tuples[i].file1_column] = 1;
                query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].fa[wl_ptr -> filters[pos].tuples[i].file1_column] = old_fa/old_da;
            }
            else
            {
                query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].da[wl_ptr -> filters[pos].tuples[i].file1_column] = 0;
                query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].fa[wl_ptr -> filters[pos].tuples[i].file1_column] = 0;
            }

            query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].la[wl_ptr -> filters[pos].tuples[i].file1_column] = wl_ptr -> filters[pos].tuples[i].limit;
            query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].ua[wl_ptr -> filters[pos].tuples[i].file1_column] = wl_ptr -> filters[pos].tuples[i].limit;
        }
        // for any other column of relation
        for (size_t j = 0; j < query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].num_columns; j++)
        {

            double old_da = query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].da[j];
            double old_fa = query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].fa[wl_ptr -> filters[pos].tuples[i].file1_column];

            // if you find the column that have been already fixed, just skip it
            if(j == wl_ptr -> filters[pos].tuples[i].file1_column)
            {

                continue;
            }
            double new_fa = old_fa;

            query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].da[j] = (old_da * (1 - powl((1 - (new_fa/old_fa_global)),(query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].fa[j]/query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].da[j]))));
            // f'c = f'a
            query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].fa[j] = query_stats[wl_ptr -> filters[pos].tuples[i].file1_ID].fa[wl_ptr -> filters[pos].tuples[i].file1_column];
        }


    }

    // Self Join and Join
    // for all predicates of current query
    for (uint64_t i = 0; i < wl_ptr -> predicates[pos].num_tuples; i++)
    {
        // For example 0.1 = 0.2 !
        if(wl_ptr -> predicates[pos].tuples[i].file1_ID == wl_ptr -> predicates[pos].tuples[i].file2_ID)
        {

        }
        // regular join for example 0.1 = 1.0
        else
        {
            uint64_t file1_ID = wl_ptr -> predicates[pos].tuples[i].file1_ID;
            uint64_t file2_ID = wl_ptr -> predicates[pos].tuples[i].file2_ID;
            uint64_t file1_column = wl_ptr -> predicates[pos].tuples[i].file1_column;
            uint64_t file2_column = wl_ptr -> predicates[pos].tuples[i].file2_column;

            double old_da_global_file1 = query_stats[file1_ID].da[file1_column];
            double old_da_global_file2 = query_stats[file2_ID].da[file2_column];
            // l'a = l'b = la = lb
            if(query_stats[file1_ID].la[file1_column] > query_stats[file2_ID].la[file2_column])
            {
                query_stats[file2_ID].la[file2_column] = query_stats[file1_ID].la[file1_column];
            }
            else
            {
                query_stats[file1_ID].la[file1_column] = query_stats[file2_ID].la[file2_column];
            }
            // u'a = u'b = ua = ub
            if(query_stats[file1_ID].ua[file1_column] > query_stats[file2_ID].ua[file2_column])
            {
                query_stats[file2_ID].ua[file2_column] = query_stats[file1_ID].ua[file1_column];
            }
            else
            {
                query_stats[file1_ID].ua[file1_column] = query_stats[file2_ID].ua[file2_column];
            }

            double n = ((query_stats[file1_ID].ua[file1_column] - query_stats[file1_ID].la[file1_column]) + 1);

            // f'a = f'b = fa*fb/n
            query_stats[file1_ID].fa[file1_column] = query_stats[file2_ID].fa[file2_column] = (query_stats[file1_ID].fa[file1_column] * query_stats[file2_ID].fa[file2_column])/n;

            // d'a = d'b = da*db/n
            query_stats[file1_ID].da[file1_column] = query_stats[file2_ID].da[file2_column] = (query_stats[file1_ID].da[file1_column] * query_stats[file2_ID].da[file2_column])/n;


            for (size_t j = 0; j < query_stats[file1_ID].num_columns; j++)
            {
                double new_da = query_stats[file1_ID].da[file1_ID];

                // if you find the column that have been already fixed, just skip it
                if(j == file1_column)
                {
                    continue;
                }

                double base = fabs(1 - (new_da/old_da_global_file1));
                double exponent = (query_stats[file1_ID].fa[j]/query_stats[file1_ID].da[j]);
                query_stats[file1_ID].da[j] = fabs(query_stats[file1_ID].da[j] * (1 - powl((base),exponent)));

                // f'c = f'a
                query_stats[file1_ID].fa[j] = query_stats[file1_ID].fa[file1_column];
            }

            for (size_t j = 0; j < query_stats[file2_ID].num_columns; j++)
            {
                double new_da = query_stats[file2_ID].da[file2_column];

                // if you find the column that have been already fixed, just skip it
                if(j == file2_column)
                {
                    continue;
                }

                double base = fabs(1 - (new_da/old_da_global_file2));
                double exponent = (query_stats[file2_ID].fa[j]/query_stats[file2_ID].da[j]);

                query_stats[file2_ID].da[j] = (query_stats[file2_ID].da[j] * fabs(1 - powl(base, exponent)));

                // f'c = f'a
                query_stats[file2_ID].fa[j] = query_stats[file2_ID].fa[file2_column];
            }
        }
    }
}
