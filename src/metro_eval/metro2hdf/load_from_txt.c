#include "load_from_txt.h"
#include <stdlib.h>
#include <string.h>

void init_dataset(Dataset *dset) {
    dset->data = NULL;
    dset->rows = 0;
    dset->cols = 0;
}

void free_dataset(Dataset *dset) {
    free(dset->data);
    dset->data = NULL;
    dset->rows = 0;
}
