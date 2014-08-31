#!/usr/bin/env python
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name="grapple",
<<<<<<< Updated upstream
    version="0.1.2",
=======
    version="0.1.1",
>>>>>>> Stashed changes
    description="Ripple ledger extractor",
    author="Jack Peterson",
    author_email="<jack@tinybike.net>",
    maintainer="Jack Peterson",
    maintainer_email="<jack@tinybike.net>",
    license="MIT",
    url="https://github.com/tensorjack/grapple",
<<<<<<< Updated upstream
    download_url = 'https://github.com/tensorjack/grapple/tarball/0.1.2',
=======
    download_url = 'https://github.com/tensorjack/grapple/tarball/0.1.1',
>>>>>>> Stashed changes
    packages=["grapple"],
    install_requires=["psycopg2", "websocket-client", "numpy", "pandas"],
    keywords = ["ripple", "rippled", "ledger", "download", "data"]
)
