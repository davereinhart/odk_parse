#from __future__ import print_function

import json
#import urllib
import boto3
from collections import defaultdict
from xml.etree import cElementTree as ET

s3 = boto3.client('s3')

def etree_to_dict(t):
    d = {t.tag: {} if t.attrib else None}
    children = list(t)
    if children:
        dd = defaultdict(list)
        for dc in map(etree_to_dict, children):
            for k, v in dc.items():
                dd[k].append(v)
        d = {t.tag: {k:v[0] if len(v) == 1 else v for k, v in dd.items()}}
    if t.attrib:
        d[t.tag].update(('@' + k, v) for k, v in t.attrib.items())
    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            if text:
              d[t.tag]['#text'] = text
        else:
            d[t.tag] = text
    return d
    
def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key'].encode('utf8')   # urllib.unquote_plus(...)
    
    try:
        response = s3.get_object(Bucket=bucket,Key=key)
        contents = response['Body'].read()
        
        tree = ET.XML(contents)
        tree_dict = etree_to_dict(tree)
        
        myjson = json.dumps(tree_dict, ensure_ascii=False)
        mykey = tree_dict['data']['meta']['instanceID']
        
        client = boto3.client('dynamodb', region_name='us-west-2')
        response = client.put_item(TableName='swca_odk_submissions', Item={'uri':{'S':mykey},'value':{'S':myjson}})
        
        return 'done'
    except Exception as e:
        #print(e)
        #print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

