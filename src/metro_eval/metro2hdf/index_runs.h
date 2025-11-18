#ifndef INDEX_RUNS_H
#define INDEX_RUNS_H

#include <stddef.h>

typedef struct {
    char* name;
    signed long long start_line;
    signed long long num_rows;
} MetroASCIIStep;

typedef struct {
    MetroASCIIStep* steps;
    char* name;
    size_t count;
    size_t capacity;
} MetroASCIIScan;

typedef struct {
    char* path;
    char* channel;
    MetroASCIIScan* scans;
    size_t count;
    size_t capacity;
} MetroASCIIFile;

typedef struct {
    char* num;
    MetroASCIIFile* ascii_files;
    size_t count;
    size_t capacity;
} MetroRun;

typedef struct {
    MetroRun* runs;
    size_t count;
    size_t capacity;
} MetroIndex;

#endif /* INDEX_RUNS_H */
