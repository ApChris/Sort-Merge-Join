#include "../include/work.h"

work_line * WorkLineInit()
{
	work_line wl_struct;
	work_line * wl_ptr = &wl_struct;

	if ((wl_ptr = ((work_line * )malloc(sizeof(work_line)))) == NULL)
	{
		perror("workLineInit.c, first malloc");
		exit(-1);
	}

	wl_ptr -> parameters = NULL;
	wl_ptr -> predicates = NULL;
	wl_ptr -> filters = NULL;
    wl_ptr -> selects = NULL;

    wl_ptr-> num_predicates = 0;
    wl_ptr-> num_selects = 0;
    wl_ptr-> num_parameters = 0;
    wl_ptr-> num_filters = 0;
	return wl_ptr;
}

void PredicateRelInit(work_line * wl_ptr)
{
    if(wl_ptr -> num_predicates == 0)
    {
        if ((wl_ptr -> predicates = ((predicate_rel * )malloc(sizeof(predicate_rel)))) == NULL)
        {
            perror("predicate_rel.c, first malloc");
            exit(-1);
        }

        wl_ptr -> predicates[0].tuples = NULL;
        wl_ptr -> predicates[0].num_tuples = 0;
        wl_ptr -> num_predicates = 1;
    }
    else
    {
        if ((wl_ptr -> predicates = ((predicate_rel * )realloc(wl_ptr -> predicates,sizeof(predicate_rel)*(wl_ptr -> num_predicates+1)))) == NULL)
        {
            perror("predicate_rel.c, realloc");
            exit(-1);
        }
        wl_ptr -> predicates[wl_ptr -> num_predicates].tuples = NULL;
        wl_ptr -> predicates[wl_ptr -> num_predicates].num_tuples = 0;
        wl_ptr -> num_predicates++;
    }
}

void Push_Predicates(work_line * wl_ptr, uint64_t file1_ID, uint64_t file1_column, uint64_t file2_ID, uint64_t file2_column, uint64_t counter)
{

	if (counter == 1)
	{
		if((wl_ptr -> predicates[wl_ptr -> num_predicates - 1].tuples = (predicate_tuple*)malloc(sizeof(predicate_tuple))) == NULL)
        {
            perror("push_predicates.c, first malloc");
            exit(-1);
        }
		wl_ptr -> predicates[wl_ptr -> num_predicates - 1].tuples[wl_ptr -> predicates[wl_ptr -> num_predicates - 1].num_tuples].file1_ID = file1_ID;
        wl_ptr -> predicates[wl_ptr -> num_predicates - 1].tuples[wl_ptr -> predicates[wl_ptr -> num_predicates - 1].num_tuples].file1_column = file1_column;
        wl_ptr -> predicates[wl_ptr -> num_predicates - 1].tuples[wl_ptr -> predicates[wl_ptr -> num_predicates - 1].num_tuples].file2_ID = file2_ID;
        wl_ptr -> predicates[wl_ptr -> num_predicates - 1].tuples[wl_ptr -> predicates[wl_ptr -> num_predicates - 1].num_tuples].file2_column = file2_column;

        wl_ptr -> predicates[wl_ptr -> num_predicates - 1].num_tuples = 1;
    }
	else
	{
		if((wl_ptr -> predicates[wl_ptr -> num_predicates - 1].tuples = (predicate_tuple*)realloc(wl_ptr -> predicates[wl_ptr -> num_predicates - 1].tuples, (sizeof(predicate_tuple)*(wl_ptr -> predicates[wl_ptr -> num_predicates - 1].num_tuples+1)))) == NULL)
        {
            perror("push_predicates.c, realloc");
            exit(-1);
        }
        wl_ptr -> predicates[wl_ptr -> num_predicates - 1].tuples[wl_ptr -> predicates[wl_ptr -> num_predicates - 1].num_tuples].file1_ID = file1_ID;
        wl_ptr -> predicates[wl_ptr -> num_predicates - 1].tuples[wl_ptr -> predicates[wl_ptr -> num_predicates - 1].num_tuples].file1_column = file1_column;
        wl_ptr -> predicates[wl_ptr -> num_predicates - 1].tuples[wl_ptr -> predicates[wl_ptr -> num_predicates - 1].num_tuples].file2_ID = file2_ID;
        wl_ptr -> predicates[wl_ptr -> num_predicates - 1].tuples[wl_ptr -> predicates[wl_ptr -> num_predicates - 1].num_tuples].file2_column = file2_column;

		wl_ptr -> predicates[wl_ptr -> num_predicates - 1].num_tuples++;
	}
}


////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void FiltersRelInit(work_line * wl_ptr)
{
    if(wl_ptr -> num_filters == 0)
    {
        if ((wl_ptr -> filters = ((filter_rel * )malloc(sizeof(filter_rel)))) == NULL)
        {
            perror("filter_rel.c, first malloc");
            exit(-1);
        }

        wl_ptr -> filters[0].tuples = NULL;
        wl_ptr -> filters[0].num_tuples = 0;
        wl_ptr -> num_filters = 1;
    }
    else
    {
        if ((wl_ptr -> filters = ((filter_rel * )realloc(wl_ptr -> filters,sizeof(filter_rel)*(wl_ptr -> num_filters+1)))) == NULL)
        {
            perror("filter_rel.c, realloc");
            exit(-1);
        }
        wl_ptr -> filters[wl_ptr -> num_filters].tuples = NULL;
        wl_ptr -> filters[wl_ptr -> num_filters].num_tuples = 0;
        wl_ptr -> num_filters++;
    }
}

void Push_Filters(work_line * wl_ptr, uint64_t file1_ID, uint64_t file1_column, char symbol, uint64_t limit, uint64_t counter)
{

	if (counter == 1)
	{
		if((wl_ptr -> filters[wl_ptr -> num_filters - 1].tuples = (filter_tuple*)malloc(sizeof(filter_tuple))) == NULL)
        {
            perror("push_filters.c, first malloc");
            exit(-1);
        }
		wl_ptr -> filters[wl_ptr -> num_filters - 1].tuples[wl_ptr -> filters[wl_ptr -> num_filters - 1].num_tuples].file1_ID = file1_ID;
        wl_ptr -> filters[wl_ptr -> num_filters - 1].tuples[wl_ptr -> filters[wl_ptr -> num_filters - 1].num_tuples].file1_column = file1_column;
        wl_ptr -> filters[wl_ptr -> num_filters - 1].tuples[wl_ptr -> filters[wl_ptr -> num_filters - 1].num_tuples].symbol = symbol;
        wl_ptr -> filters[wl_ptr -> num_filters - 1].tuples[wl_ptr -> filters[wl_ptr -> num_filters - 1].num_tuples].limit = limit;

        wl_ptr -> filters[wl_ptr -> num_filters - 1].num_tuples = 1;
    }
	else
	{
		if((wl_ptr -> filters[wl_ptr -> num_filters - 1].tuples = (filter_tuple*)realloc(wl_ptr -> filters[wl_ptr -> num_filters - 1].tuples, (sizeof(filter_tuple)*(wl_ptr -> filters[wl_ptr -> num_filters - 1].num_tuples+1)))) == NULL)
        {
            perror("push_filters.c, realloc");
            exit(-1);
        }
        wl_ptr -> filters[wl_ptr -> num_filters - 1].tuples[wl_ptr -> filters[wl_ptr -> num_filters - 1].num_tuples].file1_ID = file1_ID;
        wl_ptr -> filters[wl_ptr -> num_filters - 1].tuples[wl_ptr -> filters[wl_ptr -> num_filters - 1].num_tuples].file1_column = file1_column;
        wl_ptr -> filters[wl_ptr -> num_filters - 1].tuples[wl_ptr -> filters[wl_ptr -> num_filters - 1].num_tuples].symbol = symbol;
        wl_ptr -> filters[wl_ptr -> num_filters - 1].tuples[wl_ptr -> filters[wl_ptr -> num_filters - 1].num_tuples].limit = limit;

		wl_ptr -> filters[wl_ptr -> num_filters - 1].num_tuples++;
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

void SelectsRelInit(work_line * wl_ptr)
{
    if(wl_ptr -> num_selects == 0)
    {
        if ((wl_ptr -> selects = ((select_rel * )malloc(sizeof(select_rel)))) == NULL)
        {
            perror("selects_rel.c, first malloc");
            exit(-1);
        }

        wl_ptr -> selects[0].tuples = NULL;
        wl_ptr -> selects[0].num_tuples = 0;
        wl_ptr -> num_selects = 1;
    }
    else
    {
        if ((wl_ptr -> selects = ((select_rel * )realloc(wl_ptr -> selects,sizeof(select_rel)*(wl_ptr -> num_selects+1)))) == NULL)
        {
            perror("selects_rel.c, realloc");
            exit(-1);
        }
        wl_ptr -> selects[wl_ptr -> num_selects].tuples = NULL;
        wl_ptr -> selects[wl_ptr -> num_selects].num_tuples = 0;
        wl_ptr -> num_selects++;
    }
}

void Push_Selects(work_line * wl_ptr, uint64_t file1_ID, uint64_t file1_column, uint64_t counter)
{

	if (counter == 1)
	{
		if((wl_ptr -> selects[wl_ptr -> num_selects - 1].tuples = (select_tuple*)malloc(sizeof(select_tuple))) == NULL)
        {
            perror("push_selects.c, first malloc");
            exit(-1);
        }
		wl_ptr -> selects[wl_ptr -> num_selects - 1].tuples[wl_ptr -> selects[wl_ptr -> num_selects - 1].num_tuples].file1_ID = file1_ID;
        wl_ptr -> selects[wl_ptr -> num_selects - 1].tuples[wl_ptr -> selects[wl_ptr -> num_selects - 1].num_tuples].file1_column = file1_column;

        wl_ptr -> selects[wl_ptr -> num_selects - 1].num_tuples = 1;
    }
	else
	{
		if((wl_ptr -> selects[wl_ptr -> num_selects - 1].tuples = (select_tuple*)realloc(wl_ptr -> selects[wl_ptr -> num_selects - 1].tuples, (sizeof(select_tuple)*(wl_ptr -> selects[wl_ptr -> num_selects - 1].num_tuples+1)))) == NULL)
        {
            perror("push_selects.c, realloc");
            exit(-1);
        }
        wl_ptr -> selects[wl_ptr -> num_selects - 1].tuples[wl_ptr -> selects[wl_ptr -> num_selects - 1].num_tuples].file1_ID = file1_ID;
        wl_ptr -> selects[wl_ptr -> num_selects - 1].tuples[wl_ptr -> selects[wl_ptr -> num_selects - 1].num_tuples].file1_column = file1_column;

		wl_ptr -> selects[wl_ptr -> num_selects - 1].num_tuples++;
	}
}

/////////////////////////////////////////////////////////////////////////////////////////////////////


void ParametersRelInit(work_line * wl_ptr)
{
    if(wl_ptr -> num_parameters == 0)
    {
        if ((wl_ptr -> parameters = ((select_rel * )malloc(sizeof(select_rel)))) == NULL)
        {
            perror("filter_rel.c, first malloc");
            exit(-1);
        }

        wl_ptr -> parameters[0].tuples = NULL;
        wl_ptr -> parameters[0].num_tuples = 0;
        wl_ptr -> num_parameters = 1;
    }
    else
    {
        if ((wl_ptr -> parameters = ((select_rel * )realloc(wl_ptr -> parameters,sizeof(select_rel)*(wl_ptr -> num_parameters+1)))) == NULL)
        {
            perror("filter_rel.c, realloc");
            exit(-1);
        }
        wl_ptr -> parameters[wl_ptr -> num_parameters].tuples = NULL;
        wl_ptr -> parameters[wl_ptr -> num_parameters].num_tuples = 0;
        wl_ptr -> num_parameters++;
    }
}

void Push_Parameters(work_line * wl_ptr, uint64_t file1_ID, uint64_t counter)
{

	if (counter == 1)
	{
		if((wl_ptr -> parameters[wl_ptr -> num_parameters - 1].tuples = (select_tuple*)malloc(sizeof(select_tuple))) == NULL)
        {
            perror("push_parameters.c, first malloc");
            exit(-1);
        }
		wl_ptr -> parameters[wl_ptr -> num_parameters - 1].tuples[wl_ptr -> parameters[wl_ptr -> num_parameters - 1].num_tuples].file1_ID = file1_ID;

        wl_ptr -> parameters[wl_ptr -> num_parameters - 1].num_tuples = 1;
    }
	else
	{
		if((wl_ptr -> parameters[wl_ptr -> num_parameters - 1].tuples = (select_tuple*)realloc(wl_ptr -> parameters[wl_ptr -> num_parameters - 1].tuples, (sizeof(select_tuple)*(wl_ptr -> parameters[wl_ptr -> num_parameters - 1].num_tuples+1)))) == NULL)
        {
            perror("push_parameters.c, realloc");
            exit(-1);
        }
        wl_ptr -> parameters[wl_ptr -> num_parameters - 1].tuples[wl_ptr -> parameters[wl_ptr -> num_parameters - 1].num_tuples].file1_ID = file1_ID;

		wl_ptr -> parameters[wl_ptr -> num_parameters - 1].num_tuples++;
	}
}


void Print_Work(work_line * wl_ptr)
{
    printf("Queries number: %lu\n\n", wl_ptr -> num_predicates);

    for (size_t i = 0; i < wl_ptr -> num_parameters; i++)
    {

        for (size_t j = 0; j < wl_ptr -> parameters[i].num_tuples; j++)
        {
            printf("%lu ",wl_ptr -> parameters[i].tuples[j].file1_ID);
        }
        printf("|");

        for (size_t j = 0; j < wl_ptr -> predicates[i].num_tuples; j++)
        {
            printf("%lu.",wl_ptr -> predicates[i].tuples[j].file1_ID);
            printf("%lu=",wl_ptr -> predicates[i].tuples[j].file1_column);
            printf("%lu.",wl_ptr -> predicates[i].tuples[j].file2_ID);
            printf("%lu&",wl_ptr -> predicates[i].tuples[j].file2_column);
        }

        for (size_t j = 0; j < wl_ptr -> filters[i].num_tuples; j++)
        {
            printf("%lu.",wl_ptr -> filters[i].tuples[j].file1_ID);
            printf("%lu",wl_ptr -> filters[i].tuples[j].file1_column);
            printf("%c",wl_ptr -> filters[i].tuples[j].symbol);
            printf("%lu",wl_ptr -> filters[i].tuples[j].limit);
        }
        printf("|");
        for (size_t j = 0; j < wl_ptr -> selects[i].num_tuples; j++)
        {
            printf("%lu.",wl_ptr -> selects[i].tuples[j].file1_ID);
            printf("%lu ",wl_ptr -> selects[i].tuples[j].file1_column);

        }

        printf("\n\n");
    }
}
