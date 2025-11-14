#ifndef GLOB_METRO_FILES_H
#define GLOB_METRO_FILES_H

#include <stddef.h>

// File list
typedef struct {
    char **files;
    size_t count;
    size_t capacity;
} FileList;

// Mapping of measurement number to files
typedef struct {
    char *num;
    FileList file_list;
} NumGroup;

// List of maps
typedef struct {
    NumGroup *groups;
    size_t count;
    size_t capacity;
} Measurements;

/**
 * Groups files matching the pattern by their numeric prefix (before first '_')
 * 
 * Files must start with one or more digits followed by '_' to be included.
 * 
 * @param pattern The glob pattern to match files (e.g., "*", "*.txt")
 * @param map Pointer to Measurements to store the results (will be initialized)
 * @return 0 on success, -1 on error
 * 
 * The caller must call free_measurements() to free the allocated memory.
 */
int group_files_by_num(const char *pattern, Measurements *m);

/**
 * Free all memory allocated by a Measurements structure
 * 
 * @param map Pointer to the Measurements to free
 */
void free_measurements(Measurements *m);

#endif /* GLOB_METRO_FILES_H */
