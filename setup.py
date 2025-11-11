from setuptools import Extension, setup
from Cython.Build import cythonize
import numpy

sorting_tdc = Extension(
    name="metro_eval.data.sorting_tdc",
    sources=["src/metro_eval/data/sorting_tdc.pyx"],
    include_dirs=[numpy.get_include()],
)

setup(ext_modules=cythonize([sorting_tdc]))
