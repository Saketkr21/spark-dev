"""Shim setup.py — metadata lives in pyproject.toml.

Places ``dbt_polyglot.pth`` into the wheel's purelib (site-packages) so the patch
auto-activates on interpreter start-up.
"""
import os
import shutil

from setuptools import setup
from setuptools.command.build_py import build_py

PTH = "dbt_polyglot.pth"


class build_py_with_pth(build_py):
    def run(self):
        super().run()
        shutil.copyfile(PTH, os.path.join(self.build_lib, PTH))


setup(cmdclass={"build_py": build_py_with_pth})
