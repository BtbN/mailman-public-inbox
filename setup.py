# -*- coding: utf-8 -*-
#
# setup.py
#
# Author:   Toke Høiland-Jørgensen (toke@toke.dk)
# Date:      3 January 2021
# Copyright (c) 2021, Toke Høiland-Jørgensen
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import sys
from setuptools import setup, find_packages


setup(
    name            = 'mailman-public-inbox',
    version         = '0.1.0',
    description     = 'Mailman archiver plugin for Public Inbox',
    long_description= open("README.rst").read(),
    author='Toke Høiland-Jørgensen',
    author_email='toke@toke.dk',
    url="https://github.com/tohojo/mailman-public-inbox/",
    license         = 'GPLv3',
    keywords        = 'email',
    classifiers=[
        "Development Status :: 4 - Beta",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Topic :: Communications :: Email :: Mailing List Servers",
        "Programming Language :: Python :: 3",
        ],
    packages        = find_packages(),
    include_package_data = True,
    install_requires = [
        'setuptools',
        'mailman',
        'zope.interface',
        ],
    )
