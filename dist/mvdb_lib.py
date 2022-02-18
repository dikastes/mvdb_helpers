import csv
import json
import urllib.request

def get_items(filename):
    """Returns a list of works from csv file"""
    path = '../csv/'
    with open(path + filename) as file:
        reader = list(csv.DictReader(file))
    return reader

def get_gnd_info(gndId, i = 0):
    """Returns a json formatted work using the gndId"""
    print(f'{i} Hole {gndId}')
    stem = 'https://data.slub-dresden.de/source/gnd_marc21/'

    try:
        with urllib.request.urlopen(stem + gndId) as response:
            data = json.loads(response.read())
    except:
        print(f'HTTP-Fehler')
        return {}

    return data


def update_command(table_name, updates):
    """Returns update command for dictlist of form 
    [ { uid: [ { key: key, val: val, typ: str|int }, ... ] }, ... ] """

    def decode_val(key, val, typ = 'str'):
        if typ == 'str':
            return f"`{key}` = '{val}'"
        return f"`{key}` = {val}"
    
    def get_vals(update):
        return ", ".join([ decode_val(val['key'], val['val'], val['typ']) 
            for val in list(update.values())[0] if val['val']])

    return "\n".join([ f"UPDATE `{table_name}` " +
        f"SET {vals} " +
        f"WHERE `uid` = {list(update.keys())[0]};"
        for update in updates if (vals := get_vals(update)) ])

def insert_command(table_name, keys, values):
    """Returns insert command, values must have form
    [ [ { typ: str|int, val: val }, ... ], ... ]"""

    def list_to_comma(l):
        def decode_val(v):
            quot = "'"
            escquot = "\\\'"
            if v['val'] is None:
                return ""
            if v['typ'] == 'str':
                return f"'{v['val'].replace(quot, escquot)}'"
            if v['typ'] == 'row':
                return f"`{v['val']}`"
            return v['val']

        return ", ".join([ decode_val(v) for v in l ])

    def parenthesize(l):
        return f"({list_to_comma(l)})"

    def get_vals(values):
        return ",\n".join([(parenthesize(value)) 
            for value in values ])

    return f"INSERT INTO `{table_name}` " + \
        f"{parenthesize(keys)}\nVALUES\n" + \
        f"{get_vals(values)};" 
