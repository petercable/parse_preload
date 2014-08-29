#!/usr/bin/env python

import os
import sqlite3
import logging
import sys
from parse_preload import create_db, load_paramdicts
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

dbfile = 'preload.db'

def get_logger():
    logger = logging.getLogger('driver_control')
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)
    return logger

log = get_logger()

def streams_to_xml(stream_dict):
    root = Element('streamDefinitions')
    for stream in stream_dict.itervalues():
        params = stream.parameter_ids
        if params is None: continue
        params = params.split(',')
        streamdef = SubElement(root, 'streamDefinition', attrib={'streamName':stream.name})
        for param in params:
            child = SubElement(streamdef, 'parameterId')
            child.text = param
    open('streams.xml', 'w').write(
        minidom.parseString(tostring(root, encoding='UTF-8')).toprettyxml(encoding='UTF-8'))


def main():
    if not os.path.exists(dbfile):
        conn = sqlite3.connect(dbfile)
        create_db(conn)

    conn = sqlite3.connect(dbfile)
    stream_dict = load_paramdicts(conn)[1]
    streams_to_xml(stream_dict)

main()

