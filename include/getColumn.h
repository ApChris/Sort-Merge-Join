#ifndef _GETCOLUMN_H
#define _GETCOLUMN_H

#include <stdio.h>
#include <stdlib.h>
#include "../include/relation.h"

void getColumn(uint64_t ** array, uint64_t rows, uint64_t selected_column, relation *rel);

#endif
