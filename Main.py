import xml.etree.cElementTree as ET
from collections import defaultdict
import pprint
import re
import codecs
import json
import schema
import csv
import cerberus
import pandas as pd
import sqlalchemy
import os


dataset="boston_massachusetts.osm"





def count_tags(dataset):
    tags={}
    for event,element in ET.iterparse(dataset):
        if element.tag in tags:
            tags[element.tag]+=1
        else:
            tags[element.tag]=1
    return tags

print "count_tags:"
print(count_tags(dataset))





lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')


def key_type(element, keys):
    if element.tag == "tag":
        if lower.match(element.attrib['k']):
            keys['lower'] += 1
        elif lower_colon.match(element.attrib['k']):
            keys['lower_colon'] += 1
        elif problemchars.match(element.attrib['k']):
            keys['problemchars'] += 1
        else:
            keys['other'] += 1

    return keys


def process_map(filename):
    keys = {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0}
    for _, element in ET.iterparse(filename):
        keys = key_type(element, keys)

    return keys

print "process_map"
print process_map(dataset)





def get_user(element):
    return element.get("uid")


def process_users(dataset):
    users = []
    for _, element in ET.iterparse(dataset):
        if element.tag == "node" or element.tag == "way" or element.tag == "relation":
            val = get_user(element)
            if val in users:
                continue
            else:
                users.append(val)

    return users


print "process_users"
users = process_users(dataset)
print users[0:7]




street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)

expected = ["Court", "Place", "Square", "Lane", "Trail", "Parkway", "Commons", "Way",
            "Alley", "Steeg", "Avenue", "Laan", "Boulevard", "Kringweg", "Close", "Crescent", "Singel",
            "Drive", "Rylaan", "Place", "Oord", "Road", "Weg", "Street", "Straat"]

mapping = {"St": "Street",
           "ST": "Street",
           "st": "Street",
           "st.": "Street",
           "st,": "Street",
           "street": "Street",
           'Sq.': 'Square',
           "Ave": "Avenue",
           "ave": "Avenue",
           'Ave.': 'Avenue',
           "Rd.": "Road",
           "Rd": "Road",
           "Cresent": "Crescent",
           "drive": "Drive",
           'HIghway': 'Highway',
           'Hwy': 'Highway',
           }


def audit_street_type(street_types, street_name):
    m = street_type_re.search(street_name)
    if m:
        street_type = m.group()
        if street_type not in expected:
            street_types[street_type].add(street_name)


def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")


def audit(osmfile):
    osm_file = open(osmfile, "r")
    street_types = defaultdict(set)
    for event, elem in ET.iterparse(osm_file, events=("start",)):

        if elem.tag == "node" or elem.tag == "way":
            for tag in elem.iter("tag"):
                if is_street_name(tag):
                    audit_street_type(street_types, tag.attrib['v'])
                    tag.attrib['v'] = update_name(tag.attrib['v'], mapping)
    return street_types


def update_name(name, mapping):
    for street_type in mapping:
        if street_type in name:
            name = re.sub(r'\b' + street_type + r'\b\.?', mapping[street_type], name)
    return name

print "audit_dataset"
print audit(dataset)

OSM_PATH = "example.osm"

NODES_PATH = "nodes.csv"
NODE_TAGS_PATH = "nodes_tags.csv"
WAYS_PATH = "ways.csv"
WAY_NODES_PATH = "ways_nodes.csv"
WAY_TAGS_PATH = "ways_tags.csv"

LOWER_COLON = re.compile(r'^([a-z]|_)+:([a-z]|_)+')
PROBLEMCHARS = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

SCHEMA = schema.schema

# Make sure the fields order in the csvs matches the column order in the sql table schema
NODE_FIELDS = ['id', 'lat', 'lon', 'user', 'uid', 'version', 'changeset', 'timestamp']
NODE_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_FIELDS = ['id', 'user', 'uid', 'version', 'changeset', 'timestamp']
WAY_TAGS_FIELDS = ['id', 'key', 'value', 'type']
WAY_NODES_FIELDS = ['id', 'node_id', 'position']


def load_new_tag(element, secondary, default_tag_type):
    """
    Load a new tag dict to go into the list of dicts for way_tags, node_tags
    """
    new = {}
    new['id'] = element.attrib['id']
    if ":" not in secondary.attrib['k']:
        new['key'] = secondary.attrib['k']
        new['type'] = default_tag_type
    else:
        post_colon = secondary.attrib['k'].index(":") + 1
        new['key'] = secondary.attrib['k'][post_colon:]
        new['type'] = secondary.attrib['k'][:post_colon - 1]
    new['value'] = secondary.attrib['v']
    return new


def shape_element(element, node_attr_fields=NODE_FIELDS, way_attr_fields=WAY_FIELDS,
                  problem_chars=PROBLEMCHARS, default_tag_type='regular'):
    """Clean and shape node or way XML element to Python dict"""

    node_attribs = {}
    way_attribs = {}
    way_nodes = []
    tags = []  # Handle secondary tags the same way for both node and way elements

    # YOUR CODE HERE

    if element.tag == "node" or element.tag == "way":
        for tag in element.iter("tag"):
            if is_street_name(tag):
                tag.attrib['v'] = update_name(tag.attrib['v'], mapping)

    if element.tag == 'node':
        for attribute, value in element.attrib.iteritems():
            if attribute in node_attr_fields:
                node_attribs[attribute] = value

        for secondary in element.iter():
            if secondary.tag == 'tag':
                if problem_chars.match(secondary.attrib['k']) is not None:
                    continue
                else:
                    new_tag = load_new_tag(element, secondary, default_tag_type)
                    tags.append(new_tag)
        return {'node': node_attribs, 'node_tags': tags}

    elif element.tag == 'way':
        for attribute, value in element.attrib.iteritems():
            if attribute in way_attr_fields:
                way_attribs[attribute] = value

        counter = 0
        for secondary in element.iter():
            if secondary.tag == 'tag':
                if problem_chars.match(secondary.attrib['k']) is not None:
                    continue
                else:
                    new_tag = load_new_tag(element, secondary, default_tag_type)
                    tags.append(new_tag)
            if secondary.tag == 'nd':
                new_node = {}
                new_node['id'] = element.attrib['id']
                new_node['node_id'] = secondary.attrib['ref']
                new_node['position'] = counter
                counter += 1
                way_nodes.append(new_node)
        return {'way': way_attribs, 'way_nodes': way_nodes, 'way_tags': tags}


# ================================================== #
#               Helper Functions                     #
# ================================================== #
def get_element(osm_file, tags=('node', 'way', 'relation')):
    """Yield element if it is the right type of tag"""

    context = ET.iterparse(osm_file, events=('start', 'end'))
    _, root = next(context)
    for event, elem in context:
        if event == 'end' and elem.tag in tags:
            yield elem
            root.clear()


def validate_element(element, validator, schema=SCHEMA):
    """Raise ValidationError if element does not match schema"""
    if validator.validate(element, schema) is not True:
        field, errors = next(validator.errors.iteritems())
        message_string = "\nElement of type '{0}' has the following errors:\n{1}"
        error_string = pprint.pformat(errors)

        raise Exception(message_string.format(field, error_string))


class UnicodeDictWriter(csv.DictWriter, object):
    """Extend csv.DictWriter to handle Unicode input"""

    def writerow(self, row):
        super(UnicodeDictWriter, self).writerow({
                                                    k: (v.encode('utf-8') if isinstance(v, unicode) else v) for k, v in
                                                    row.iteritems()
                                                    })

    def writerows(self, rows):
        for row in rows:
            self.writerow(row)


# ================================================== #
#               Main Function                        #
# ================================================== #
def process_map(file_in, validate):
    """Iteratively process each XML element and write to csv(s)"""

    with codecs.open(NODES_PATH, 'w') as nodes_file, \
            codecs.open(NODE_TAGS_PATH, 'w') as nodes_tags_file, \
            codecs.open(WAYS_PATH, 'w') as ways_file, \
            codecs.open(WAY_NODES_PATH, 'w') as way_nodes_file, \
            codecs.open(WAY_TAGS_PATH, 'w') as way_tags_file:

        nodes_writer = UnicodeDictWriter(nodes_file, NODE_FIELDS)
        node_tags_writer = UnicodeDictWriter(nodes_tags_file, NODE_TAGS_FIELDS)
        ways_writer = UnicodeDictWriter(ways_file, WAY_FIELDS)
        way_nodes_writer = UnicodeDictWriter(way_nodes_file, WAY_NODES_FIELDS)
        way_tags_writer = UnicodeDictWriter(way_tags_file, WAY_TAGS_FIELDS)

        nodes_writer.writeheader()
        node_tags_writer.writeheader()
        ways_writer.writeheader()
        way_nodes_writer.writeheader()
        way_tags_writer.writeheader()

        validator = cerberus.Validator()

        for element in get_element(file_in, tags=('node', 'way')):
            el = shape_element(element)
            if el:
                # el = update_name(element, mapping)
                if validate is True:
                    validate_element(el, validator)

                if element.tag == 'node':
                    nodes_writer.writerow(el['node'])
                    node_tags_writer.writerows(el['node_tags'])
                elif element.tag == 'way':
                    ways_writer.writerow(el['way'])
                    way_nodes_writer.writerows(el['way_nodes'])
                    way_tags_writer.writerows(el['way_tags'])

data=process_map(dataset, validate=False)



csv_nodes = pd.read_csv("nodes.csv", encoding="utf-8")
print csv_nodes.head()

csv_nodes_tags = pd.read_csv("nodes_tags.csv", encoding="utf-8")
csv_nodes_tags.head()
print csv_nodes_tags.loc[csv_nodes_tags.loc[:,'key'] == "phone",:]

def only_digits(string_to_filter):
    """
    Filters the phone number and leaves only digits 0-9
    """
    return ''.join(c for c in string_to_filter if c in "0123456789")

def correct_phone_number(ph_number):
    """
    Transforms the phone number into the ###-###-#### format
    """
    result = only_digits(str(ph_number))
    if len(result) > 0 and result[0] == "1":
        result = result[1:]
    if len(result) == 10:
        return result[0:3] + "-" + result[3:6] + "-" + result[6:10]
    else:
        return "incorrect_num"


csv_nodes_tags.loc[csv_nodes_tags.loc[:,'key'] == "phone", "value"] = \
csv_nodes_tags.loc[csv_nodes_tags.loc[:,'key'] == "phone", "value"].apply(correct_phone_number)

csv_ways = pd.read_csv("ways.csv", encoding="utf-8")
print csv_ways.head()

csv_ways_nodes = pd.read_csv("ways_nodes.csv", encoding="utf-8")
print csv_ways_nodes.head()

csv_ways_tags = pd.read_csv("ways_tags.csv", encoding="utf-8")
print csv_ways_tags.head()




disk_engine = sqlalchemy.create_engine('sqlite:///boston_db.db')

csv_nodes.to_sql('nodes', disk_engine, if_exists='replace', index=False)
csv_nodes_tags.to_sql('nodes_tags', disk_engine, if_exists='replace', index=False)
csv_ways.to_sql('ways', disk_engine, if_exists='replace', index=False)
csv_ways_nodes.to_sql('ways_nodes', disk_engine, if_exists='replace', index=False)
csv_ways_tags.to_sql('ways_tags', disk_engine, if_exists='replace', index=False)






print "boston_massachusetts.osm: " + str(os.path.getsize(dataset) / 1024 / 1024) + " MB"
print "nodes.csv: " + str(os.path.getsize("nodes.csv") / 1024 / 1024) + " MB"
print "nodes_tags.csv: " + str(os.path.getsize("nodes_tags.csv") / 1024 / 1024) + " MB"
print "ways.csv: " + str(os.path.getsize("ways.csv") / 1024 / 1024) + " MB"
print "ways_nodes.csv: " + str(os.path.getsize("ways_nodes.csv") / 1024 / 1024) + " MB"
print "ways_tags.csv: " + str(os.path.getsize("ways_tags.csv") / 1024 / 1024) + " MB"
print "boston_db.db: " + str(os.path.getsize("boston_db.db") / 1024 / 1024) + " MB"






result = pd.read_sql_query("""
SELECT COUNT(DISTINCT users.uid) AS num_of_unique_users
FROM (SELECT uid FROM Nodes UNION ALL SELECT uid FROM Ways) AS users;
""", disk_engine)
print result


result = pd.read_sql_query("""
SELECT users.user, COUNT(*) as num_of_contributions
FROM (SELECT user FROM Nodes UNION ALL SELECT user FROM Ways) users
GROUP BY users.user
ORDER BY num_of_contributions DESC
LIMIT 10;
""", disk_engine)
print result


result = pd.read_sql_query("""
SELECT timestamp FROM Nodes UNION SELECT timestamp FROM Ways
ORDER BY timestamp
LIMIT 1;
""", disk_engine)
print result


result = pd.read_sql_query("""
SELECT COUNT(*) AS number_of_nodes FROM nodes;
""", disk_engine)
print result


result = pd.read_sql_query("""
SELECT COUNT(*) AS number_of_ways FROM ways
""", disk_engine)
print result


result = pd.read_sql_query("""
SELECT nodes_tags.value AS bank,
    COUNT(*) as num
FROM nodes_tags
    JOIN (SELECT DISTINCT id FROM nodes_tags WHERE value='bank') ids
    ON nodes_tags.id=ids.id
WHERE nodes_tags.key='name'
GROUP BY nodes_tags.value
ORDER BY num DESC
LIMIT 10;
""", disk_engine)
print result


result = pd.read_sql_query("""
SELECT nodes_tags.value AS cousine,
    COUNT(*) as num
FROM nodes_tags
    JOIN (SELECT DISTINCT id FROM nodes_tags WHERE value='restaurant') ids
    ON nodes_tags.id=ids.id
WHERE nodes_tags.key='cuisine'
GROUP BY nodes_tags.value
ORDER BY num DESC
LIMIT 10;
""", disk_engine)
print result


result = pd.read_sql_query("""
SELECT nodes_tags.value AS amenity,
    COUNT(*) as num
FROM nodes_tags
    JOIN (SELECT DISTINCT id FROM nodes_tags) ids
    ON nodes_tags.id=ids.id
WHERE nodes_tags.key='amenity'
GROUP BY nodes_tags.value
ORDER BY num DESC
LIMIT 10;
""", disk_engine)
print result


result = pd.read_sql_query("""
SELECT nodes_tags.value AS religion,
    COUNT(*) as num
FROM nodes_tags
    JOIN (SELECT DISTINCT id FROM nodes_tags WHERE value='place_of_worship') ids
    ON nodes_tags.id=ids.id
WHERE nodes_tags.key='religion'
GROUP BY nodes_tags.value
ORDER BY num DESC
LIMIT 10;
""", disk_engine)
print result


result = pd.read_sql_query("""
SELECT nodes_tags.value AS chain,
    COUNT(*) as num
FROM nodes_tags
    JOIN (SELECT DISTINCT id FROM nodes_tags WHERE value='fast_food') ids
    ON nodes_tags.id=ids.id
WHERE nodes_tags.key='name'
GROUP BY nodes_tags.value
ORDER BY num DESC
LIMIT 10;
""", disk_engine)
print result