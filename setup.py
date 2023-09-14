#!/usr/bin/env python
from setuptools import setup

from pathlib import Path
long_description = (Path(__file__).parent / "README.md").read_text()



setup(
    name="grapple",
    version="0.3.0",
    description="Deprecated. Use wagtail-grapple instead",
    author="Jack Peterson",
    author_email="<jack@tinybike.net>",
    license="MIT",
    url="https://github.com/torchbox/wagtail-grapple",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=["grapple"],
    install_requires=[],
    keywords = ["deprecated"],
    classifiers=["Development Status :: 7 - Inactive"],
)
