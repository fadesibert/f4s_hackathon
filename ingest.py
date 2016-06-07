from elasticsearch import Elasticsearch
import pdb
import csv
import pprint
import certifi
import urllib3
from datetime import datetime
from csv import reader

ES="https://5ca187159d24f2f9ba937c42158c7ab7.us-east-1.aws.found.io:9243"
ES="5ca187159d24f2f9ba937c42158c7ab7.us-east-1.aws.found.io"

def debug():
  pdb.set_trace()

def ingest_file(filename, name):
  list_of_dicts=[]
  with open (filename, 'r') as csvfile:
    reader=csv.DictReader(csvfile)
    for row in reader:
      json={
        "_index" : name,
        "_type" : name,
        "_source" : row
      }
      list_of_dicts.append(json)
  debug()
  x=1

es = Elasticsearch(
  [ES], 
  http_auth=("admin", ""
  port=9243,
  use_ssl=True,
  verify_certs=True, 
  ca_certs=certifi.where()
)

doc = {
    'author': 'kimchy',
    'text': 'Elasticsearch: cool. bonsai cool.',
    'timestamp': datetime.now(),
}
res = es.index(index="test-index", doc_type='tweet', id=1, body=doc)

debug()

ingest_file("./data/company.csv", "company")
