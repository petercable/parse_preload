#!/usr/bin/env python

import os
import sqlite3
import logging
import sys
from parse_preload import create_db, load_paramdicts, load_paramdefs
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

from pprint import pprint

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

def massage_value(x):
    if x is None:
        return ''
    return unicode(x)


def streams_to_xml(stream_dict, outputfile):
    root = Element('streamDefinitions')
    for stream in stream_dict.itervalues():
        params = stream.parameter_ids
        if params is None: continue
        params = params.split(',')
        streamdef = SubElement(root, 'streamDefinition', attrib={'streamName': stream.name})
        for param in params:
            child = SubElement(streamdef, 'parameterId')
            child.text = param
    outputfile.write(
        minidom.parseString(tostring(root, encoding='UTF-8')).toprettyxml(encoding='UTF-8'))


# 'id, scenario, hid, parameter_type, value_encoding, units, display_name, precision, '
# 'parameter_function_id, parameter_function_map, dpi'
def params_to_xml(param_dict, outputfile):
    root = Element('parameterContainer')
    for param in param_dict.itervalues():
        SubElement(root, 'parameter',
                   attrib={'pd_id': massage_value(param.id),
                           'name': massage_value(param.name),
                           'type': massage_value(param.parameter_type),
                           'unit': massage_value(param.units),
                           'fill': massage_value(param.fill_value),
                           'encoding': massage_value(param.value_encoding),
                           'precision': massage_value(param.precision)}
        )
    outputfile.write(
        minidom.parseString(tostring(root, encoding='UTF-8')).toprettyxml(encoding='UTF-8'))

def main():
    if not os.path.exists(dbfile):
        conn = sqlite3.connect(dbfile)
        create_db(conn)

    conn = sqlite3.connect(dbfile)
    stream_dict = load_paramdicts(conn)[1]
    param_dict = load_paramdefs(conn)
    streams_to_xml(stream_dict, open('streams.xml', 'w'))
    params_to_xml(param_dict, open('params.xml', 'w'))


main()

