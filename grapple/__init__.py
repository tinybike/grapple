from __future__ import division
import sys
try:
    import cdecimal
    sys.modules["decimal"] = cdecimal
except:
    pass

__title__      = "grapple"
__version__    = "0.2"
__author__     = "Jack Peterson"
__copyright__  = "Copyright 2014, Dyffy Inc."
__license__    = "MIT"
__maintainer__ = "Jack Peterson"
__email__      = "jack@tinybike.net"

from .grapple import Grapple
