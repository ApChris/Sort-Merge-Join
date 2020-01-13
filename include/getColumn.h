#ifndef _GETCOLUMN_H
#define _GETCOLUMN_H

#include <string.h>
#include "relation.h"
#include "metadata.h"
#include "work.h"
#include "statistics.h"

// functions to get input

void GetColumn_FromFILE(const char * filename, relation *rel);

// metadata * Read_Init_Binary(const char * filename);
metadata * Read_Init_Binary(const char * filename, char * fileFlag, uint64_t * num_rows, statistics * stats);
work_line * Read_Work(const char * filename);

// uint64_t self_join_check(char * str1, char * str2);
char * split(char * str, const char * delim);
void Write_To_File(metadata * md, char * filename,uint64_t pos);
#endif
