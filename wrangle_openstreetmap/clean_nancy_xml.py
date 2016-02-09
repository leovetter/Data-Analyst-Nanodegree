import xml.etree.cElementTree as ET
import pprint
from pymongo import MongoClient

# Needed to update the names of cities that are misspelled
city_mapping = {
    "Vandoeuvre-les-Nancy": "Vandoeuvre-lès-Nancy",
    "Vandœuvre-lès-Nancy": "Vandoeuvre-lès-Nancy",
    "Vandoeuvre les Nancy": "Vandoeuvre-lès-Nancy",
    "Villiers-lès-Nancy": "Villers-lès-Nancy"
}

#  Needed to update the names of the streets that are misspelled
street_mapping = {

    'Avenue Général Leclerc': 'Avenue du Général Leclerc',
    'Rue du Général Leclerc': 'Avenue du Général Leclerc',
    "Rue Philipe Martin": "Rue Phillippe Martin"
}


def shape_element(element):
    """
    Format a xml element to a proper json format

    :type element: xml.etree.ElementTree.Element
    :param element: element to format to a proper json format
    """

    node = {}
    if element.tag == "node" or element.tag == "way":

        # Create sub-element 'created' with the informations about the creation of the element
        attribs = element.attrib
        wanted_keys = ['version', 'changeset', 'timestamp', 'user', 'uid']
        created = dict([(i, attribs[i]) for i in wanted_keys if i in attribs])
        node['created'] = created

        # Create sub-element 'position' with the informations about the location of the element
        if 'lat' in attribs:
            node['pos'] = [float(attribs['lat']), float(attribs['lon'])]

        # Assign additional informations
        if 'visible' in attribs:
            node['visible'] = attribs['visible']
        node['type'] = element.tag
        node['id'] = attribs['id']

        # Create sub-element 'adress' with the informations about the adress of the element
        address = {}
        for elem in element.iter('tag'):

            if elem.attrib['k'].startswith('addr') and len(elem.attrib['k'].split(':')) == 2:
                key = elem.attrib['k'].split(':')[1]

                # Update city and street names
                if key == 'city' and elem.attrib['v'] in city_mapping:
                    value = city_mapping[elem.attrib['v']]
                    address[key] = value
                elif key == 'street' and elem.attrib['v'] in street_mapping:
                    value = street_mapping[elem.attrib['v']]
                    address[key] = value
                else:
                    # print(elem.attrib['v'])
                    # print(elem.attrib['v'].capitalize())
                    address[key] = elem.attrib['v'].capitalize()
            else:
                node[elem.attrib['k']] = elem.attrib['v']
        if address:
            node['address'] = address


        # If the element is of type 'way' we want to store the nodes that it references
        if element.tag == "way":

           node_refs = []
           for elem in element.iter():
               if elem.tag == 'nd':
                   node_refs.append(elem.attrib['ref'])
           node['node_refs'] = node_refs

        return node

def process_map():

    # Connection to the database server
    client = MongoClient()
    db = client.nancy_map

    # Run through all the nodes of the xml files
    for _, element in ET.iterparse('nancy_france.osm', events=("start",)):

        # Format the element to a suitable format for mongodb
        el = shape_element(element)

        if el != None:
            # Insert the parsed element into the db
            if el['type'] == 'node':
                db.nodes.insert_one(el)
            elif el['type'] == 'way':
                db.ways.insert_one(el)

process_map()
