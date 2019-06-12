#!/usr/bin/env python

"""
This is a way to save the startup time when running img2py on lots of
files...
"""

from wx.tools import img2py


command_lines = [
   
    "-a -F -n setting img/process.png images.py"
    ]


if __name__ == "__main__":
    for line in command_lines:
        args = line.split()
        img2py.main(args)

