#ifndef _METADATA_H_
#define _METADATA_H_

typedef struct metadata
{
    uint64_t num_tuples;
    uint64_t num_columns;
    uint64_t * array;
    uint64_t * full_array;
}metadata;



#endif
