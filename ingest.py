from elasticsearch import Elasticsearch
import sys
import pdb
import csv
import pprint
import certifi
import urllib3
import logging
import time
from elasticsearch import helpers
from datetime import datetime
from csv import reader

ES="5ca187159d24f2f9ba937c42158c7ab7.us-east-1.aws.found.io"

## LOGGING
logging.basicConfig(level=logging.INFO, 
                    stream=sys.stdout,
                    format='%(asctime)s %(message)s') # include timestamp

logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def debug():
  pdb.set_trace()

def ingest_file(filename, name, delimiter):
  if delimiter == "pipe" :
    delim="|"
  else:
    delim=","

  logging.info("Deleting index : %s" % name)
  try:
    es.indices.delete(index=name)
  except Exception, e:
    logging.warning("Index %s does not exist" % name)
  logging.info("Completed deleting index : %s" % name)

  logging.info("Creating index : %s" % name)
  try:
    mapping={
      "mappings" : 
      {
        name  : {
          "_all" : { "enabled" : True },
          "properties" : {
            "First name" : { "type" : "string", "index" : "not_analyzed" },
            "company_hq_mp" : { "type" : "string", "index" : "not_analyzed" },
            "company_name" : { "type" : "string", "index" : "not_analyzed" },
            "company_location" : { "type" : "geo_point" },
            "industries" : { "type" : "string", "index" : "not_analyzed" },
            "company_hq_political_party" : { "type" : "string", "index" : "not_analyzed" },
            "gender" : { "type" : "string", "index" : "not_analyzed" },
            "company_tags": { "type": "string", "analyzer": "industry_synonyms" },
            "summary": { "type": "string", "analyzer": "industry_synonyms" }
          }
        }
      },
      "settings": {
        "analysis": {
          "filter": {
            "industry_filter": {
              "type": "synonym",
              "synonyms": [
                "developer, coder, hacker => programmer",
                "technician => engineer",
                "programme, program => project"
              ]
            }
          },
          "analyzer": {
            "industry_synonyms": {
              "tokenizer": "standard",
              "filter": [
                "lowercase",
                "industry_filter"
              ]
            }
          }
        }
      }
    }
    es.indices.create(index=name, body=mapping)
  except Exception, e:
    debug()
    logging.warning("Index %s does not exist" % name)
  logging.info("Created index : %s" % name)

  list_of_dicts=[]
  with open (filename, 'r') as csvfile:
    reader=csv.DictReader(csvfile, delimiter=delim)
    for row in reader:
      try:
        json={
          "_index" : name,
          "_type" : name,
          "_source" : row
        }
        #time_stamp=datetime.strptime(json["_source"]["company_updated"][:-10],"%Y-%m-%d %H:%M:%S")
        #formatted_timestamp=time_stamp.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        #json["_source"]["company_updated"]=formatted_timestamp
        json["_source"]["company_location"]={}
        json["_source"]["company_location"]["lat"]=json["_source"]["latitude"]
        json["_source"]["company_location"]["lon"]=json["_source"]["longitude"]
        del json["_source"]["latitude"]
        del json["_source"]["longitude"]
        list_of_dicts.append(json)
      except Exception, e:
        debug()
        logging.error("Ignoring record")

  if len(list_of_dicts) > 0:
    helpers.bulk(es,list_of_dicts,chunk_size=100)

## PROGRAM STARTS HERE
logging.info("Initiating connection to Elastic")
es = Elasticsearch(
  [ES], 
  ## Add Creds HERE ##
  http_auth=("readwrite", ""),
  port=9243,
  use_ssl=True,
  verify_certs=True, 
  ca_certs=certifi.where()
)
logging.info("Initiated connection to Elastic")

#ingest_file("./data/company.csv", "company")
#ingest_file("./data/businessleader.csv", "businessleader")
ingest_file("./data/data5.psv", "founders2", "pipe")

