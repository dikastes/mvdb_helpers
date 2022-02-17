import mvdb_lib as lib

class PersonList:
    persons = []
    place_list = None

    def read(self, filename):
        self.persons = [ Person(row['gnd_id'], row['uid'])
            for row in lib.get_items(filename) ]

    def generatePlaceList(self):
        place_list = PlaceList()
        for (i, person) in enumerate(self.persons):
            person.addPlaces(i, place_list)
        place_list.batchGetGnd()
        self.place_list = place_list

    def writeCommand(self, filename):
        def get_uid(place):
            if (place is None):
                return ''
            return place.uid

        updates = [ { p.uid: 
            [ { "key": "place_of_birth", "val": get_uid(p.place_of_birth), "typ": "int" },
                { "key": "place_of_death", "val": get_uid(p.place_of_death), "typ": "int" } ] }
                for p in self.persons ]
        insert_values = [ [ person.uid, str(place.uid) ] 
            for person in self.persons
            for place in person.places_of_activity ]
        #insert_values = [ item for item in sublist for sublist in insert_values ]
        insert_keys = [ "uid_local", "uid_foreign" ]
        #insert_keys = [ [ person.uid for _ in person.places_of_activity ]
            #for person in self.persons ][0]
        place_inserts_values = [ [ str(place.uid), place.name, 
            place.alt_names, place.alt_names, str(place.N), str(place.E) ]
            for place in self.place_list.places ]
        place_inserts_keys = [ "uid", "name", "alt_names", 
            "gnd_id", "latitude", "longitude" ]

        update_command = lib.update_command("tx_publisherdb_domain_model_person", updates)
        #insert_command = "\n".join( [ 
            #lib.insert_command("tx_publisherdb_person_placeofactivity_place_mm", keys, values) 
            #for (keys, values) in zip(insert_keys, insert_values) ])
        mm_command = lib.insert_command("tx_publisherdb_person_placeofactivity_place_mm",
            insert_keys, insert_values)
        place_command = lib.insert_command("tx_publisherdb_domain_model_place",
            place_inserts_keys, place_inserts_values)


        with open("../output/" + filename, "w") as file:
            file.write("use `db`;\n")
            file.write(update_command + "\n")
            file.write(mm_command + "\n")
            file.write(place_command + "\n")

class Person:
    uid = ""
    gnd_id = ""
    place_of_birth = None
    place_of_death = None
    places_of_activity = []

    def __init__(self, gnd_id, uid):
        self.gnd_id = gnd_id
        self.uid = uid

    def addPlaces(self, i, place_list):
        def is_place(typ, cell):
            return {"i": typ} in cell['__']
        def get_id(array):
            url = [ cell['0'] for cell in array['__']
                if '0' in cell.keys() and 'd-nb.info' in cell['0'] ]
            if url:
                return url[0].split('d-nb.info/gnd/')[1]

        gnd_person = lib.get_gnd_info(self.gnd_id, i)

        if '551' in gnd_person.keys():
            place_of_birth_ids = [ get_id(cell) for cell in gnd_person['551']
                if is_place("Geburtsort", cell) ]
            if place_of_birth_ids:
                self.place_of_birth = place_list.addPlace(place_of_birth_ids[0])

            place_of_death_ids = [ get_id(cell) for cell in gnd_person['551']
                if is_place("Sterbeort", cell) ]
            if place_of_death_ids:
                self.place_of_death = place_list.addPlace(place_of_death_ids[0])

            places_of_activity_ids = [ get_id(cell) for cell in gnd_person['551']
                if is_place("Sterbeort", cell) ]
            self.places_of_activity = [ place_list.addPlace(place)
                for place in places_of_activity_ids ]

class PlaceList:
    max_uid = 0
    gnd_ids = []
    places = []

    def addPlace(self, place_gndid):
        if place_gndid not in self.gnd_ids:
            self.max_uid += 1
            self.places.append(Place(self.max_uid, place_gndid))
            self.gnd_ids.append(place_gndid)
        return [ p for p in self.places if p.gnd_id == place_gndid ][0]

    def batchGetGnd(self):
        for place in self.places:
            place.getGnd()

class Place:
    uid = 0
    gnd_id = ''
    name = ''
    N = 0
    E = 0
    alt_names = ''

    def __init__(self, uid, gnd_id):
        self.uid = uid
        self.gnd_id = gnd_id

    def getGnd(self):
        def is_coord(c):
            return { 9: "A:dgx" } in [ cell for cell in c['__'] ]
        def get_coord(c, key):
            string = [ cell[key] for cell in c['__'] if key in cell.keys() ]
            numeric = float(string[1:])
            if string[0] == 'N' or string[0] == 'E':
                return numeric
            return -numeric
            
        gnd_place = lib.get_gnd_info(self.gnd_id)
        if '151' in gnd_place.keys():
            self.name = gnd_place['151'][0]['__'][0]['a']

        if '034' in gnd_place.keys():
            N = [ get_coord(c, 'f') for c in gnd_place['034'] if is_coord(c) ]
            if N:
                self.N = N[0]

            E = [ get_coord(c, 'd') for c in gnd_place['034'] if is_coord(c) ]
            if E:
                self.E = N[0]

        if '451' in gnd_place.keys():
            self.alt_names = "$".join([ c['__'][0]['a'] for c in gnd_place['451']
                if 'a' in c['__'][0] ])

def main(infile = 'tx_publisherdb_domain_model_person.csv', outfile = 'place_command.sql'):
    person_list = PersonList()
    person_list.read(infile)
    person_list.generatePlaceList()
    person_list.writeCommand(outfile)

#class PlaceBuilder:
    #person_dict = {}
    #place_dict = {}
    #activ_place_dict = {}
    #cmd = ''
#
    #def update_command(self, person):
        #def is_url(entry):
            #return 'd-nb.info' in entry
        #def is_birthplace(cell):
            #return [ entry for entry in cell if 'i' in entry.keys() and entry['i'] == 'Geburtsort' ]
        #def is_deathplace(cell):
            #return [ entry for entry in cell if 'i' in entry.keys() and entry['i'] == 'Sterbesort' ]
        #def is_activplace(cell):
            #return [ entry for entry in cell if 'i' in entry.keys() and entry['i'] == 'Wirkungsort' ]
        #def url_to_gnd(places):
            #gnds = [ p.split('info/gnd/')[1] for p in places ]
            #return gnds[0], gnds[1], gnds[2:]
#
        #birth = [ entry for entry in cell if is_url(entry) for cell in person['551'] if is_birthplace(cell) ]
        #death = [ entry for entry in cell if is_url(entry) for cell in person['551'] if is_deathplace(cell) ]
        #activ = [ entry for entry in cell if is_url(entry) for cell in person['551'] if is_activplace(cell) ]
        #birth, death, activ = url_to_gnd(birth + death + activ)
        #birth, death, activ = self.get_or_make(birth + death + activ)
        #self.cmd += f"UPDATE `tx_publisherdb_domain_model_person`"
        #
    #def get_or_make(self, places):
        #def switch(place):
            #if place in self.place_dict:
                #return self.place_dict[place]
            #else:
                #self.cmd += f"INSERT INTO `tx_publisherdb_domain_model_place` (`uid`, `gnd_id`) VALUES ('{place}')"
                #return lib.get_gnd_info(place)
        #places = [ switch(place) for place in places ]
        #return places[0], places[1], places[2:]
#
    #def read_persons(self, filename):
        #person_list = lib.get_items(filename)
        #self.person_dict = { person['gndId']: person for person in person_list }
#
    #def write_cmd(out):
        #with open(out, 'w') as file:
            #file.write(self.cmd)
#
    #def start(self, filename, out):
        #self.read_persons()
        #self.cmd = 'use `db`;\n'
        #self.cmd += 'TRUNCATE TABLE `tx_publisherdb_domain_model_place`;\n'
        #self.cmd += 'TRUNCATE TABLE `tx_publisherdb_person_placeofactivity_place_mm`;\n'
        #for person in person_dict:
            #update_command(person)
        #self.write_cmd(out)
