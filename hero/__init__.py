"""
Module to load officials into a database, from several sources:
- tournament applications
- explicit lists of history docs 

Once in the database, they can be queried and the data refreshed.

More detail to come.
"""
import config
import thegoogles
import events
import util
import official

__author__ = 'hammer'
__version__ = 0.6

# TODO: complete modularising / packaging
# 1: add a setup.py
# 2: add more documentation

# TODO: process applicants:
# 1: load each official from db (refresh it, if it's stale)
# 2: calculate eligibile positions
# 3: do per official calculations here (such as weighting)
# 4: create a per-position summary of eligible applicants

# TODO: create a way to capture eligibily requirements (per position or family)

# TODO: create a way to capture weighting model (likely a function to maximise flexibility, games in, positional weighting out)


if __name__ == '__main__':
    print "This is a HEROic module!"
