"""Entry point for hosts that start a Python app from main.py.

The real supervisor lives in start.py; this file only forwards to it so both
names work.
"""

from start import main

if __name__ == "__main__":
    main()