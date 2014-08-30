#!/usr/bin/env python
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name="grapple",
    version="0.1",
    description="Download the Ripple ledger directly from rippled.",
    author="Jack Peterson",
    author_email="<jack@tinybike.net>",
    maintainer="Jack Peterson",
    maintainer_email="<jack@tinybike.net>",
    license="MIT",
    url="https://github.com/tensorjack/grapple",
    download_url = 'https://github.com/tensorjack/grapple/tarball/0.1',
    packages=["grapple"],
    include_package_data=True,
    package_data={"grapple": ["./data/coins.json", "./bitcoin-listen"]},
    install_requires=["psycopg2",],
    keywords = ["ripple", "rippled", "download", "data"]
)
