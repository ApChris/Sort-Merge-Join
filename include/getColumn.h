#ifndef _GETCOLUMN_H
#define _GETCOLUMN_H

#include "../include/relation.h"

void GetColumn(uint64_t ** array, uint64_t rows, uint64_t selected_column, relation *rel);
void GetColumn_FromFILE(const char * filename, relation *rel);
#endif
