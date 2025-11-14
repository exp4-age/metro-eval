#ifndef LOAD_FROM_TXT_H
#define LOAD_FROM_TXT_H

#include <stddef.h>

typedef struct {
    char *name;
    char *value;
} Attribute;

typedef struct {
    float *data;
    size_t rows;
    size_t cols;
} Dataset;

typedef struct {
    Dataset *dsets;
    char **steps;
    size_t count;
} Scan;

typedef struct {
    Scan *scans;
    size_t count;
    Attribute *attrs;
} Group;

#endif /* LOAD_FROM_TXT_H */
