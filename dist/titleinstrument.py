import mvdb_lib as lib

def write_command(
    filename = 'tx_publisherdb_domain_model_work.csv', 
    out='update_titleinstruments.sql'):
    works = lib.get_items( filename )
    cmd = update_commands( works )
    path = '../output/'
    with open(path + out, 'w') as file:
        file.write('use `db`;\n' + cmd)

def update_commands(works):
    cmds = [ cmd for (i, w) in enumerate(works) if (cmd := make_command(w, i)) ]
    return '\n'.join(cmds)

def make_command(work, i):
    title_instrument = get_instruments(work, i)
    if title_instrument:
        print(title_instrument)
        return f"UPDATE `tx_publisherdb_domain_model_work` SET `title_instrument` = '{', '.join(title_instrument)}' WHERE `uid` = {work['uid']};"
    return ''

def get_instruments(work, i):
    if work.keys() and 'gnd_id' in work.keys():
        gnd_work = lib.get_gnd_info(work['gnd_id'], i)
    if gnd_work and '100' in gnd_work.keys():
        if '1_' in gnd_work['100'][0].keys():
            l = [ i['m'] for i in gnd_work['100'][0]['1_'] if 'm' in i.keys() ]
            if l and isinstance(l[0], list):
                return l[0]
            return l
    return []
