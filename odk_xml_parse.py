from __future__ import print_function

import json
import urllib
import boto3
import xml.etree.ElementTree as ET
import xml.dom.minidom as MD


s3 = boto3.client('s3')

def strip_tag(tag):
    strip_ns_tag = tag
    split_array = tag.split('}')
    if len(split_array) > 1:
        strip_ns_tag = split_array[1]
        tag = strip_ns_tag
    return tag

def xml2json(xmlstring, strip_ns=1, strip=1):

    """Convert an XML string into a JSON string."""

    elem = ET.fromstring(xmlstring)
    return elem2json(elem, strip_ns=strip_ns, strip=strip)

def elem2json(elem, strip_ns=1, strip=1):

    """Convert an ElementTree or Element into a JSON string."""

    if hasattr(elem, 'getroot'):
        elem = elem.getroot()

    #if options.pretty:
    #    return json.dumps(elem_to_internal(elem, strip_ns=strip_ns, strip=strip), sort_keys=True, indent=4, separators=(',', ': '))
    #else:
    return json.dumps(elem_to_internal(elem, strip_ns=strip_ns, strip=strip))

def elem_to_internal(elem, strip_ns=1, strip=1):
    """Convert an Element into an internal dictionary (not JSON!)."""

    d = {}
    elem_tag = elem.tag
    if strip_ns:
        elem_tag = strip_tag(elem.tag)
    else:
        for key, value in list(elem.attrib.items()):
            d['@' + key] = value

    # loop over subelements to merge them
    for subelem in elem:
        v = elem_to_internal(subelem, strip_ns=strip_ns, strip=strip)

        tag = subelem.tag
        if strip_ns:
            tag = strip_tag(subelem.tag)

        value = v[tag]

        try:
            # add to existing list for this tag
            d[tag].append(value)
        except AttributeError:
            # turn existing entry into a list
            d[tag] = [d[tag], value]
        except KeyError:
            # add a new non-list entry
            d[tag] = value
    text = elem.text
    tail = elem.tail
    if strip:
        # ignore leading and trailing whitespace
        if text:
            text = text.strip()
        if tail:
            tail = tail.strip()

    if tail:
        d['#tail'] = tail

    if d:
        # use #text element if other attributes exist
        if text:
            d["#text"] = text
    else:
        # text is the value if no attributes
        d = text or None
        return {elem_tag: d}

def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    #key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    key = event['Records'][0]['s3']['object']['key'].encode('utf8')
    
    try:
        response = s3.get_object(Bucket=bucket,Key=key)
        contents = response['Body'].read()
        
        root = ET.fromstring(contents)
        
        data = {}
        innerdoc = data[root.attrib['id']] = {}

        for child in root.iter():
            if child.text:
                value = child.text
            else:
                value = ''
            innerdoc[child.tag] = value
        
        myjson = json.dumps(data)
        
        #myjson = xml2json(contents)
        client = boto3.client('dynamodb', region_name='us-west-2')
        response = client.put_item(TableName='swca_odk_submissions', Item={'uri':{'S':key},'value':{'S':myjson}})
        
        return 'done'
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
