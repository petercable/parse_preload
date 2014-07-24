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
import os

import openpyxl
import sqlite3
import logging
import urllib
import docopt

__author__ = 'pcable'

preload_path="https://docs.google.com/spreadsheet/pub?key=0AttCeOvLP6XMdG82NHZfSEJJOGdQTkgzb05aRjkzMEE&output=xls"
assetmappings_path="https://docs.google.com/spreadsheet/pub?key=0AttCeOvLP6XMdFVUeDdoUTU0b0NFQ1dCVDhuUjY0THc&output=xls"

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

def test_parameters(conn):
    c = conn.cursor()
    c.execute("select id,parameter_function_map from parameterdefs where parameter_function_map not like ''")
    for row in c:
        try:
            eval(row[1])
        except:
            log.error('ERROR PARSING %s %s', row[0], row[1])

def main():
    options = docopt.docopt(__doc__)
    log.debug('Opening database...')

    if options['--rebuild'] or not os.path.exists(dbfile):
        conn = sqlite3.connect(dbfile)
        create_db(conn)

    conn = sqlite3.connect(dbfile)
    test_parameters(conn)

if __name__ == '__main__':
    main()


