# -*- coding: utf-8 -*-
import os
import logging
import logging.config


if not os.path.exists("log"):
    os.mkdir("log")

logging.config.fileConfig('logging.conf')

log = logging.getLogger("main")

