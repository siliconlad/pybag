"""Setup file for building Cython extensions."""
from setuptools import setup, Extension
from Cython.Build import cythonize
import sys

extensions = [
    Extension(
        "pybag.schema.cython_decoder",
        ["src/pybag/schema/cython_decoder.pyx"],
        extra_compile_args=["-O3"] if sys.platform != "win32" else ["/O2"],
    )
]

setup(
    ext_modules=cythonize(
        extensions,
        compiler_directives={
            "language_level": "3",
            "boundscheck": False,
            "wraparound": False,
            "cdivision": True,
        },
    )
)
