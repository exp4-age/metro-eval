from setuptools import Extension, setup

try:
    from Cython.Build import cythonize
    import numpy

except ImportError:
    ext_modules = []

else:
    sorting_tdc = Extension(
        name="metro_eval.cli.sort_events.sorting_tdc",
        sources=["src/metro_eval/cli/sort_events/sorting_tdc.pyx"],
        include_dirs=[numpy.get_include()],
    )

    index_ascii = Extension(
        name="metro_eval.cli.metro2hdf._index_ascii",
        sources=["src/metro_eval/cli/metro2hdf/_index_ascii.py"],
    )

    ext_modules = cythonize([sorting_tdc, index_ascii])

setup(ext_modules=ext_modules)
