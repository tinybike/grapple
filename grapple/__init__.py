from __future__ import division
import sys
try:
    import cdecimal
    sys.modules["decimal"] = cdecimal
except:
    pass

__title__      = "grapple"
__version__    = "0.1"
__author__     = "Jack Peterson"
__copyright__  = "Copyright 2014"
__license__    = "MIT"
__maintainer__ = "Jack Peterson"
__email__      = "jack@tinybike.net"
