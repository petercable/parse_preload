#!/usr/bin/env python
"""
    Usage:
        ./parse_preload.py [--rebuild]

    To run:
        create virtenv
        pip install openpyxl
        pip install docopt
        run as above...
"""
from collections import namedtuple, Counter
import json

import os
import sqlite3
import logging
import urllib

try:
    import openpyxl
    import docopt
except:
    import sys
    sys.stderr.write(__doc__)
    sys.exit(1)

__author__ = 'pcable'

preload_path="https://docs.google.com/spreadsheet/pub?key=0AttCeOvLP6XMdG82NHZfSEJJOGdQTkgzb05aRjkzMEE&output=xls"
assetmappings_path="https://docs.google.com/spreadsheet/pub?key=0AttCeOvLP6XMdFVUeDdoUTU0b0NFQ1dCVDhuUjY0THc&output=xls"

IA_SELECT = """
SELECT id, scenario, ia_driver_uri, ia_driver_module, ia_driver_class, stream_configurations, agent_default_config
FROM instrumentagent
WHERE id like 'IA%%'
"""

STREAM_SELECT = """
SELECT id, scenario, cfg_stream_type, cfg_stream_name, cfg_parameter_dictionary_name
FROM streamconfiguration
WHERE id like 'SC%'
"""

temp = 'temp.xlsx'
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

def load(f):
    return openpyxl.load_workbook(f, data_only=True)

def xlsx_to_dictionary(workbook):
    parsed = {}
    for sheet in workbook.worksheets:
        if sheet.title == 'Info':
            continue
        rows = [[cell.value for cell in row] for row in sheet.rows]
        if len(rows) <= 1: continue
        keys = rows[0]
        while keys[-1] is None:
            keys = keys[:-1]
        #log.debug('sheet: %s keys: %s', sheet.title, keys)
        parsed[sheet.title] = [keys]
        for row in rows[1:]:
            row_set = set(row)
            if len(row_set) == 1 and None in row_set: continue
            #log.debug('sheet: %s keys: %s', sheet.title, row[:len(keys)])
            parsed[sheet.title].append(row[:len(keys)])
    return parsed

def get_parameters(param_list, param_dict):
    params = {}
    for param in param_list:
        param = param_dict.get(param)
        if param is None: return
        params[param['Name']] = param
    return params

def deunicode(orig):
    if type(orig) == dict:
        d = {}
        for key, value in orig.iteritems():
            if type(value) in [dict, list]:
                value = deunicode(value)
            elif type(value) == unicode:
                try:
                    value = str(value)
                except:
                    pass
            d[str(key)] = value
        return d
    elif type(orig) == list:
        l = []
        for each in orig:
            l.append(deunicode(each))
        return l
    elif type(orig)== unicode:
        try:
            return str(orig)
        except:
            return orig

def sanitize_for_sql(row):
    subs = {
        ' ': '_',
        '-': '_',
        '/': '_',
        '(': '',
        ')': '',
    }
    new_row = []
    for val in row:
        for x,y in subs.iteritems():
            val = val.replace(x,y)
        new_row.append(val)
    return new_row

def sanitize_names(name):
    subs = {
        'Constraint': 'Constraints',
    }
    return subs.get(name, name)

def create_table(conn, name, row):
    row = sanitize_for_sql(row)
    log.debug('CREATE TABLE: %s %r', name, row)
    c = conn.cursor()
    try:
        c.execute('DROP TABLE %s' % name)
    except:
        pass
    c.execute('CREATE TABLE %s (%s)' % (name, ', '.join(row)))
    conn.commit()

def populate_table(conn, name, rows):
    log.debug('POPULATE TABLE: %s NUM ROWS: %d', name, len(rows))
    c = conn.cursor()
    c.executemany('INSERT INTO %s VALUES (%s)' % (name, ','.join(['?']*len(rows[0]))), rows)
    conn.commit()

def create_db(conn):
    for path in [preload_path, assetmappings_path]:
        log.debug('Fetching file from %s', path)
        urllib.urlretrieve(path, temp)
        log.debug('Parsing excel file')
        workbook = deunicode(xlsx_to_dictionary(load(temp)))
        for sheet in workbook:
            log.debug('Creating table: %s', sheet)
            name = sanitize_names(sheet)
            create_table(conn, name, workbook[sheet][0])
            populate_table(conn, name, workbook[sheet][1:])
    os.unlink(temp)

def test_param_function_map(conn):
    c = conn.cursor()
    c.execute("select id,parameter_function_map from parameterdefs where parameter_function_map not like ''")
    for row in c:
        try:
            if row[0].startswith('PD'):
                obj = eval(row[1])
                json_string = json.dumps(obj)
                # log.error('PARSED %s %s %r %r %r', str(row[1]) == json_string, row[0], row[1], obj, json_string)
                # c.execute("update parameterdefs set parameter_function_map=%s where id='%r'" % (json_string, row[0]))
        except Exception as e:
            log.error('ERROR PARSING %s %r %s', row[0], row[1], e)

def test_stream(conn, _id):
    c = conn.cursor()
    c.execute("""SELECT scenario, ia_driver_uri, ia_driver_module, ia_driver_class,
                        stream_configurations, agent_default_config
                 FROM instrumentagent
                 WHERE id='%s'""" % _id)
    row = c.fetchone()
    log.debug(row)
    scenario, uri, module, _class, streams, rates = row
    if streams is None:
        log.error('NO STREAMS DEFINED FOR IA: %s', _id)
    else:
        for stream in streams.split(","):
            stream = load_stream(conn, stream)

StreamConfig = namedtuple('StreamConfig', 'id, scenario, stream_type, stream_name, dict_name')
# CREATE TABLE StreamConfiguration (Scenario, COMMENT, ID, cfg_stream_type,
# cfg_stream_name, cfg_parameter_dictionary_name, attr_display_name, comment2);
def load_streams(conn):
    log.debug('Loading Stream Configurations')
    c = conn.cursor()
    c.execute(STREAM_SELECT)
    streams = map(StreamConfig._make, c.fetchall())
    stream_dict = {stream.id:stream for stream in streams}
    if len(streams) != len(stream_dict):
        log.warn('Duplicate StreamConfig record(s) found')
        counter = Counter([stream.id for stream in streams])
        for k, v in counter.iteritems():
            if v == 1:
                continue
            log.warn('ID: %s COUNT: %d', k, v)
    return stream_dict

InstrumentAgent = namedtuple('InstrumentAgent', 'id, scenario, uri, module, driver_class, streams, config')
# CREATE TABLE InstrumentAgent (Scenario, ID, owner_id, lcstate, org_ids, instrument_model_ids,
# ia_name, ia_description, ia_agent_version, ia_driver_uri, ia_driver_module, ia_driver_class,
# stream_configurations, agent_default_config);
# MASSP_A|IA_MASSP_A||DEPLOYED_AVAILABLE|MF_RSN|MASSPA|MASSP Agent|MASSP Agent||
# http://sddevrepo.oceanobservatories.org/releases/harvard_massp_ooicore-0.0.3-py2.7.egg|
# mi.instrument.harvard.massp.ooicore.driver|InstrumentDriver|SC1,SC330,SC331,SC332,SC333|
# aparam_pubrate_config.raw:5,
# aparam_pubrate_config.massp_mcu_status:5,
# aparam_pubrate_config.massp_turbo_status:5,
# aparam_pubrate_config.massp_rga_status:5,
# aparam_pubrate_config.massp_rga_sample:5
def load_agents(conn):
    log.debug('Loading Instrument Agents')
    c = conn.cursor()
    c.execute(IA_SELECT)
    agents = map(InstrumentAgent._make, c.fetchall())
    agent_dict = {agent.id:agent for agent in agents}
    if len(agents) != len(agent_dict):
        log.warn('Duplicate InstrumentAgent record found')
        counter = Counter([agent.id for agent in agents])
        for k, v in counter.iteritems():
            if v == 1:
                continue
            log.warn('ID: %s COUNT: %d', k, v)
    return agent_dict

def test_stream_configs(conn):
    c = conn.cursor()
    instrument_agents = load_agents(conn)
    streams = load_streams(conn)
    for agent in instrument_agents.itervalues():
        #log.debug('Checking %s', agent)
        check_for_missing_values(agent)
        # each agent should have just one scenario, verify
        assert len(agent.scenario.split(',')) == 1
        if agent.streams is not None:
            for stream in agent.streams.split(','):
                stream = streams.get(stream)
                if stream is None:
                    log.error('UNDEFINED STREAM: %s', stream)
                    continue
                if not agent.scenario in stream.scenario.split(','):
                    if not 'BETA' in stream.scenario.split(','):
                        log.error('Scenario %s missing from %s', agent.scenario, stream)

def check_for_missing_values(data):
    for k, v in data._asdict().iteritems():
        if v is None:
            log.warn('Missing value (%s) from %s %s', k, type(data).__name__, data.id)

def main():
    options = docopt.docopt(__doc__)
    log.debug('Opening database...')

    if options['--rebuild'] or not os.path.exists(dbfile):
        conn = sqlite3.connect(dbfile)
        create_db(conn)

    conn = sqlite3.connect(dbfile)
    test_param_function_map(conn)
    test_stream_configs(conn)

if __name__ == '__main__':
    main()


