#ifndef _GETCOLUMN_H
#define _GETCOLUMN_H

#include <stdio.h>
#include <stdlib.h>
#include "../include/relation.h"

void getColumn(int64_t ** array, int64_t rows, int64_t selected_column, relation *rel);

#endif
