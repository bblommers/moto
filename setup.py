#!/usr/bin/env python
from __future__ import unicode_literals
from io import open
import os
import re
from setuptools import setup

# Borrowed from pip at https://github.com/pypa/pip/blob/62c27dee45625e1b63d1e023b0656310f276e050/setup.py#L11-L15
here = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
    # intentionally *not* adding an encoding option to open, See:
    #   https://github.com/pypa/virtualenv/issues/201#issuecomment-3145690
    with open(os.path.join(here, *parts), "r") as fp:
        return fp.read()


def get_version():
    version_file = read("moto", "__init__.py")
    version_match = re.search(
        r'^__version__ = [\'"]([^\'"]*)[\'"]', version_file, re.MULTILINE
    )
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


if __name__ == '__main__':
    setup(name="moto", version=get_version())
