#ifndef _GETCOLUMN_H
#define _GETCOLUMN_H

#include <string.h>
#include "relation.h"
#include "metadata.h"

// functions to get input
void GetColumn(uint64_t ** array, uint64_t rows, uint64_t selected_column, relation *rel);
void GetColumn_FromFILE(const char * filename, relation *rel);
#endif
