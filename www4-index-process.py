import warc
import os
import sys
import traceback
from bs4 import BeautifulSoup
from elasticsearch import helpers
from elasticsearch import Elasticsearch, TransportError
#import argparse

# Configuration
input_dir = './input' # Put .warc.gz files under ./input directory
host = 'localhost'
port = 9200
index_name = 'better_eng'
create = False

es = Elasticsearch(hosts=[{"host": host, "port": port}],
                   retry_on_timeout=True, max_retries=10)

# FOR TEST ONLY!!!
#es.indices.delete(index=index_name, ignore=[400, 404])

# Settings for initialising ES index
settings = {
    'settings': {
        'index': {
            # Optimize for loading; this gets reset when we're done.
            'refresh_interval': '-1',
            'number_of_shards': '5',
            'number_of_replicas': '0',
        },
        # Set up a custom unstemmed analyzer.
        'analysis': {
            'analyzer': {
                'english_exact': {
                    'tokenizer': 'standard',
                    'filter': [
                        'lowercase'
                    ]
                }
            }
        }
    },
    'mappings': {
        # 'docs' is a type for ES 6.X
        # For ES 7.x, please annotate 'docs' field
        #'docs':{
            'properties': {
                'text': {
                    # text is stemmed; text.exact is not.
                    'type': 'text',
                    'analyzer': 'english',
                    'fields': {
                        'exact': {
                            'type': 'text',
                            'analyzer': 'english_exact'
                        }
                    }
                },
                'orig': {
                    'type': 'object',
                    'enabled': False, # don't index this
                }
            }
        #}
    }
}

# Create ES index
if create or not es.indices.exists(index=index_name):
    try:
        es.indices.create(index=index_name, body=settings)
    except TransportError as e:
        print(e.info)
        sys.exit(-1)

# Read warc files
def read_warc_file(file_path):
    print('Reading ' + file_path)
    with warc.open(file_path) as f:
        for record in f:
            try:
                type = record["WARC-Type"]
                if type != 'response':
                    continue
                url = record['WARC-Target-URI']
                uid = record['WARC-RECORD-ID'].replace("<urn:uuid:", "").replace(">", "")
                content = record.payload.read()
                soup = BeautifulSoup(content, 'html.parser')
                [script.extract() for script in soup.findAll('script')]
                [style.extract() for style in soup.findAll('style')]

                if soup.body is not None:
                    content_body = soup.body.get_text()
                    text = content_body.replace("\t", " ").replace("\n", " ").replace("  ", " ")
                else:
                    text = ""

                if soup.title is not None:
                    title = soup.title.string
                else:
                    title = text[:30]

                source_block = {
                    "title":title,
                    "orig":{
                        "derived-metadata":{
                            "text":text,
                            "warc-file":uid
                        },
                        "WARC-Target-URI":url
                    },
                    "text":text,
                    #"PERSON":"",
                    #"ORG":"",
                    #"GPE":""
                }
                print("Indexing: "+uid)
                try:
                    es.index(index = index_name, id = uid, body = source_block)
                except:
                    print("error")
            except:
                traceback.print_exc()

for root, dirs, files in os.walk(input_dir):
    for file_name in files:
        if(file_name.endswith('.warc.gz')):
            file_path = input_dir + '/' + file_name
            read_warc_file(file_path)

'''
es.indices.put_settings(index=index_name,
                        body={'index': { 'refresh_interval': '1s',
                                         'number_of_replicas': '1',
                        }})
'''

