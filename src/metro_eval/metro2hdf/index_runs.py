import cython
from pathlib import Path


def group_files(file_list: list[Path]):
    for file_path in file_list:
        file_name = file_path.name
