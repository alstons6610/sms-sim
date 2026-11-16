#!/usr/bin/env python3
"""Convenience launcher so the simulation can be run as `python run.py`."""

import sys

from sms_sim.main import main

if __name__ == "__main__":
    sys.exit(main())
