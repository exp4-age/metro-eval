from setuptools import Extension, setup
from Cython.Build import cythonize
import numpy

sorting_tdc = Extension(
    name="metro_eval.data.sorting_tdc",
    sources=["src/metro_eval/data/sorting_tdc.pyx"],
    include_dirs=[numpy.get_include()],
    optional=True,
)

index_ascii = Extension(
    name="metro_eval.metro2hdf._index_ascii",
    sources=["src/metro_eval/metro2hdf/_index_ascii.py"],
    optional=True,
)

setup(ext_modules=cythonize([sorting_tdc, index_ascii]))
