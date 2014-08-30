#!/usr/bin/env python
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name="grapple",
    version="0.1",
    description="Download the Ripple ledger from rippled.",
    author="Jack Peterson",
    author_email="<jack@tinybike.net>",
    maintainer="Jack Peterson",
    maintainer_email="<jack@tinybike.net>",
    license="MIT",
    url="https://github.com/tensorjack/grapple",
    download_url = 'https://github.com/tensorjack/grapple/tarball/0.1',
    packages=["grapple"],
    install_requires=["psycopg2", "websocket-client", "numpy", "pandas"],
    keywords = ["ripple", "rippled", "ledger", "download", "data"]
)
