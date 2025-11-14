#include "glob_metro_files.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <glob.h>
#include <ctype.h>

int add_file(FileList *list, const char *filename) {
    if (list->count >= list->capacity) {
        size_t new_capacity = list->capacity == 0 ? 4 : list->capacity * 2;
        char **new_files = realloc(list->files, sizeof(char*) * new_capacity);
        if (new_files == NULL) {
            return -1;
        }
        list->files = new_files;
        list->capacity = new_capacity;
    }

    list->files[list->count] = strdup(filename);
    if (list->files[list->count] == NULL) {
        return -1;
    }
    list->count++;
    return 0;
}

void free_file_list(FileList *list) {
    for (size_t i = 0; i < list->count; i++) {
        free(list->files[i]);
    }
    free(list->files);
    list->files = NULL;
    list->count = 0;
    list->capacity = 0;
}

NumGroup* find_group(Measurements *m, const char *num) {
    // Search in reverse order for efficiency in case of
    // already sorted files
    if (m->count == 0) {
        return NULL;
    }

    for (size_t i = m->count; i > 0; i--) {
        if (strcmp(m->groups[i-1].num, num) == 0) {
            return &m->groups[i-1];
        }
    }
    return NULL;
}

NumGroup* add_group(Measurements *m, const char *num) {
    printf("Adding group for: %s\n", num);
    if (m->count >= m->capacity) {
        size_t new_capacity = m->capacity == 0 ? 4 : m->capacity * 2;
        NumGroup *new_groups = realloc(m->groups, sizeof(NumGroup) * new_capacity);
        if (new_groups == NULL) {
            return NULL;
        }
        m->groups = new_groups;
        m->capacity = new_capacity;
    }

    NumGroup *group = &m->groups[m->count];
    group->num = strdup(num);
    if (group->num == NULL) {
        return NULL;
    }
    group->file_list.files = NULL;
    group->file_list.count = 0;
    group->file_list.capacity = 0;

    m->count++;
    return group;
}

void free_measurements(Measurements *m) {
    for (size_t i = 0; i < m->count; i++) {
        free(m->groups[i].num);
        free_file_list(&m->groups[i].file_list);
    }
    free(m->groups);
    m->groups = NULL;
    m->count = 0;
    m->capacity = 0;
}

char* extract_num(const char *filename) {
    if (filename == NULL) {
        return NULL;
    }

    // Find the last '/' to get just the filename
    const char* last_slash = strrchr(filename, '/');
    const char* base_name = last_slash ? last_slash + 1 : filename;

    // Check if the filename starts with digits
    size_t i = 0;
    while (base_name[i] != '\0' && isdigit(base_name[i])) {
        i++;
    }

    // Must have at least one digit and the next character must be '_'
    if (i == 0 || base_name[i] != '_') {
        return NULL;
    }

    // Extract the num
    char *num = malloc(i + 1);
    if (num == NULL) {
        return NULL;
    }

    strncpy(num, base_name, i);
    num[i] = '\0';

    return num;
}

int group_files_by_num(const char *pattern, Measurements *m) {
    glob_t globbuf;
    int ret;

    if (pattern == NULL) {
        fprintf(stderr, "Error: NULL parameter provided\n");
        return -1;
    }

    m->groups = NULL;
    m->count = 0;
    m->capacity = 0;

    memset(&globbuf, 0, sizeof(globbuf));

    // Perform the glob operation
    ret = glob(pattern, GLOB_APPEND, NULL, &globbuf);

    if (ret != 0) {
        if (ret == GLOB_NOMATCH) {
            globfree(&globbuf);
            return 0;
        }
        fprintf(stderr, "Error: glob failed\n");
        globfree(&globbuf);
        return -1;
    }

    // Process each matched file
    for (size_t i = 0; i < globbuf.gl_pathc; i++) {
        const char *filepath = globbuf.gl_pathv[i];

        // Extract the num prefix
        char *num = extract_num(filepath);
        if (num == NULL) {
            // Doesn't match pattern - discard
            continue;
        }

        // Find or create the group
        NumGroup *group = find_group(m, num);
        if (group == NULL) {
            group = add_group(m, num);
            if (group == NULL) {
                fprintf(stderr, "Error: Failed to add NumGroup\n");
                free(num);
                globfree(&globbuf);
                free_measurements(m);
                return -1;
            }
        }

        // Add the file to the group
        if (add_file(&group->file_list, filepath) != 0) {
            fprintf(stderr, "Error: Failed to add file to list\n");
            free(num);
            globfree(&globbuf);
            free_measurements(m);
            return -1;
        }

        free(num);
    }

    globfree(&globbuf);
    return 0;
}

int main(int argc, char *argv[]) {
    const char *pattern = "*";  // Default: all files in current directory

    if (argc > 1) {
        pattern = argv[1];
    }

    Measurements m;

    if (group_files_by_num(pattern, &m) == 0) {
        for (size_t i = 0; i < m.count; i++) {
            printf("Processing group with prefix '%s':\n", m.groups[i].num);
            for (size_t j = 0; j < m.groups[i].file_list.count; j++) {
                printf("  - %s\n", m.groups[i].file_list.files[j]);
            }
        }

        // Clean up
        free_measurements(&m);
    } else {
        fprintf(stderr, "Error: Failed to group files\n");
        return 1;
    }

    return 0;
}
