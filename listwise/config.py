# -*- coding: utf-8 -*-
"""
Created on Thu Oct 27 10:28:51 2016

@author: zbarge
"""

import os
from configparser import ConfigParser

CONFIG_INI_PATH = os.path.join(os.getcwd(), 'config.ini')

config = ConfigParser()
config.read(CONFIG_INI_PATH)

