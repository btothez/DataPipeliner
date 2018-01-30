#!/usr/bin/env python3

import sys
import packages.archiver as archiver
import packages.processor as processor
import packages.importer as importer
import packages.constructor as constructor
import packages.materializer as materializer
import bugsnag
import packages.config as config
import logging
import os


config = config.Config()

bugsnag.configure(
    api_key=config.bugsnag_creds['api_key'],
    project_root=config.bugsnag_creds['project_root']
)


try:
    os.remove(config.logging_file)
except OSError:
    pass

logging.basicConfig(filename=config.logging_file, level=logging.DEBUG)

if len(sys.argv) == 1 or sys.argv[1] \
    not in ['archive', 'process', 'import', 'construct', 'materialize']:
    print("USE app.py [archive|process|import|construct|materialize]")
    sys.exit()

i = 1
while i < len(sys.argv):
    next_arg = sys.argv[i]
    i += 1

    if next_arg == 'archive':
        archiver = archiver.Archiver()
        archiver.run()

    elif next_arg == 'process':
        processor = processor.Processor()
        processor.run()

    elif next_arg == 'import':
        importer = importer.Importer()
        importer.run()

    elif next_arg == 'construct':
        constructor = constructor.Constructor()
        constructor.run()

    elif next_arg == 'materialize':
        materializer = materializer.Materializer()
        materializer.run()
