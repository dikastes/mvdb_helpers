#!/usr/bin/python

import mvdb_lib as lib

def getId(item):
    return [ cell['0'][22:] for cell in item['__'] if '0' in cell.keys() and 'd-nb.info/' in cell['0'] ]

def getAlt(item):
    return [ cell['p'] for cell in item['__'] if 'p' in cell.keys() ]

def getRepl(item):
    return [ cell['v'] for cell in item['__'] if 'v' in cell.keys() ]

def getName(item):
    if getRepl(item) and getAlt(item):
        repl = getRepl(item)[0][16:]
        return repl + '>' + getAlt(item)[0]
    return None

def getInstrumentIds(work):
    if '382' in work.keys():
        return '$'.join([ result[0] for i in work['382'] if (result := getId(i)) != [] ])
    return None

def getAltInstrumentNames(work):
    if '382' in work.keys():
        return '$'.join([ result for i in work['382'] if (result := getName(i)) != None ])
    return None

def getGenreIds(work):
    if '380' in work.keys():
        return '$'.join([ result[0] for g in work['380'] if (result := getId(g)) != [] ])
    return None

def wrapCommand(db, command):
    return f'use `{db}`;\n' + command

def updateCommand(table, work, i):
    uid = work['uid']
    gndid = work['gnd_id']
    gndwork = lib.get_gnd_info(gndid, i)
    if not gndwork:
        return ''

    c = {}
    c['instrument_ids'] = getInstrumentIds(gndwork)
    c['genre_ids'] = getGenreIds(gndwork)
    c['alt_instrument_names'] = getAltInstrumentNames(gndwork)

    return '\n'.join( [ f"UPDATE `{table}` set `{key}` = '{value}' WHERE `uid` = {uid} and `{key}` = '';" for (key, value) in c.items() ] )
    #updates = ', '.join( [ f"`{key}` = '{value}'" for (key, value) in c.items() ] )

    #return f"UPDATE `{table}` SET {updates} WHERE `uid` = {uid};"

def makeCommand(
        table = 'tx_publisherdb_domain_model_work', 
        db = 'db', 
        filename = 'tx_publisherdb_domain_model_work.csv'):
    return wrapCommand(db, '\n'.join( 
        [ text for (i, w) in enumerate(lib.get_items(filename))
            if not (text := updateCommand(table, w, i)) == '' ] ) )

def writeCommand(
        infile = 'tx_publisherdb_domain_model_work.csv',
        table = 'tx_publisherdb_domain_model_work',
        db = 'db',
        outfile = 'update_gi.sql'):
    with open(outfile, 'w') as file:
        file.write( makeCommand(filename = infile, db = db, table = table) )
