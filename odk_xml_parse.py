import xml
import json

tree = xml.etree.ElementTree.parse('sample_2016-10-04_15-05-09.xml')
root = tree.getroot()

data = {}
innerdoc = data[root.attrib['id']] = {}
for child in root:
    if child.text:
        value = child.text
    else:
        value = ''
    innerdoc[child.tag] = value

myjson = json.dumps(data)
print myjson
