#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#################################################################################################################
# Author: Alexander                                                                                             #
# based on : https://github.com/trevorndodds/elasticsearch-metrics/blob/master/Grafana/elasticsearch2elastic.py #
#################################################################################################################


###########
# Modules #
###########

# timer and date
import datetime,time
# Log status
import logging
from logging.handlers import RotatingFileHandler

# format
import json, yaml

# HTTP library
import requests
from requests.auth import HTTPBasicAuth

# System
import os, sys


############
# VARIBLES #
############


logfilename = '/var/log/elastic_metrics.log'

config_file = '/data/elastic_metrics/config.yml'
certeficate = 'ca.crt'


# Read cluster
elasticServer = 'https://127.0.0.1:9200'
interval = 60

# Write Cluster
elasticIndex = 'elasticsearch_metrics'
elasticMonitoringCluster = 'https://127.0.0.1:9200'




# Enable Elasticsearch Security
# read_username and read_password for read ES cluster information
# write_username and write_passowrd for write monitor metric to ES.
read_es_security_enable = False
read_username = "read_username"
read_password = "read_password"

write_es_security_enable = False
write_username = "write_username"
write_password = "write_password"


##############
# Procedures #
##############

####################
# Read config file #
####################
def read_config_file(config_file):
    # get config. exit when error
    try:
        logger.info("Start working. Read configuration file.")
        conf_open=yaml.load(open(config_file),Loader=yaml.SafeLoader)
        #conf = yaml.load(open(options.conf_path), Loader=yaml.SafeLoader)
    except Exception as e:
        logger.exception(" Cant read configuration file {}. Error: {}".format(str(config_file), str(e)))        
        raise

#############
# Read Data #
#############
def handle_urlopen(urlData, read_username, read_password):
    if read_es_security_enable: 
      try:
        response=requests.get(urlData , auth=HTTPBasicAuth(read_username, read_password),verify=False) #,timeout=10
        #print (response)  
        return response
      except Exception as e:
        #print "Error:  {0}".format(str(e))
        logger.exception("Connection problem. Error: {}".format(str(e)))
    else:
      try:
        response=requests.get(urlData , verify=False) #,timeout=10  
        return response
      except Exception as e:
        logger.exception("Connection problem. Error: {}".format(str(e)))
        #print "Error:  {0}".format(str(e))

#############
# Post Data #
#############
def post_data(data):
    utc_datetime = datetime.datetime.utcnow()
    url_parameters = {'cluster': elasticMonitoringCluster, 'index': elasticIndex,
        'index_period': utc_datetime.strftime("%Y.%m.%d"), }
    url = "%(cluster)s/%(index)s-%(index_period)s/message" % url_parameters
    headers = {'content-type': 'application/json'}
    try:
        #req = urllib2.Request(url, headers=headers, data=json.dumps(data))
        if write_es_security_enable:
            response=requests.post(url , auth=HTTPBasicAuth(write_username, write_password),verify=False,data=json.dumps(data),headers=headers)
            logger.info("Data posted\nResponse: {}\nAnswer:\n{}".format(str(response),str(response.text)))
        else:
            #response = urllib2.urlopen(req)
            response=requests.post(url, verify=False,data=json.dumps(data),headers=headers)
            logger.info("Data posted\nResponse: {}\nAnswer:\n{}".format(str(response),str(response.text)))
    except Exception as e:
        logger.exception("Connection problem. Error: {}".format(str(e)))
        #print "Error:  {0}".format(str(e))



###################
# Get cluster health and return Clustername
################
def fetch_clusterhealth():
    try:
        utc_datetime = datetime.datetime.utcnow()
        endpoint = "/_cluster/health"
        urlData = elasticServer + endpoint
        response = handle_urlopen(urlData,read_username,read_password)
        jsonData = json.loads(response.text)
        clusterName = jsonData['cluster_name']
        jsonData['@timestamp'] = str(utc_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3])
        if jsonData['status'] == 'green':
            jsonData['status_code'] = 0
        elif jsonData['status'] == 'yellow':
            jsonData['status_code'] = 1
        elif jsonData['status'] == 'red':
            jsonData['status_code'] = 2
        post_data(jsonData)
        return clusterName
    except IOError as err:
        #print "IOError: Maybe can't connect to elasticsearch."
        logger.exception("Connection problem. Error: {}".format(str(err)))
        clusterName = "unknown"
        return clusterName

######################
# Get cluster stats
####################
def fetch_clusterstats():
    utc_datetime = datetime.datetime.utcnow()
    endpoint = "/_cluster/stats"
    urlData = elasticServer + endpoint
    response = handle_urlopen(urlData,read_username,read_password)
    jsonData = json.loads(response.text)
    jsonData['@timestamp'] = str(utc_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3])
    post_data(jsonData)

##################
# Get node stats #
##################


def fetch_nodestats(clusterName):
    utc_datetime = datetime.datetime.utcnow()
    endpoint = "/_cat/nodes?v&h=n"
    urlData = elasticServer + endpoint
    response = handle_urlopen(urlData,read_username,read_password)
    nodes = response.text[1:-1].strip().split('\n')
    for node in nodes:
        endpoint = "/_nodes/%s/stats" % node.rstrip()
        urlData = elasticServer + endpoint
        response = handle_urlopen(urlData,read_username,read_password)
        jsonData = json.loads(response.text)
        nodeID = list(jsonData['nodes'])
        try:
            jsonData['nodes'][nodeID[0]]['@timestamp'] = str(utc_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3])
            jsonData['nodes'][nodeID[0]]['cluster_name'] = clusterName
            newJsonData = jsonData['nodes'][nodeID[0]]
            post_data(newJsonData)
        except Exception as e:
            logger.exception("Can't post node data . Error {}. Continue".format(str(e)))
            continue


def fetch_indexstats(clusterName):
    utc_datetime = datetime.datetime.utcnow()
    endpoint = "/_stats"
    urlData = elasticServer + endpoint
    response = handle_urlopen(urlData,read_username,read_password)
    jsonData = json.loads(response.text)
    jsonData['_all']['@timestamp'] = str(utc_datetime.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3])
    jsonData['_all']['cluster_name'] = clusterName
    post_data(jsonData['_all'])




##############
# Fetch data #
##############

def main():
    print("Start cicle in {}".format(str(datetime.datetime.now())))
    try:
        logger.info("Get cluster health and name")
        clusterName = fetch_clusterhealth()
    except Exception as e:
        logger.exception("Can't get cluster health . Error {}. Emergency service halt".format(str(e)))
    if clusterName != "unknown":
        try:
            logger.info("Get cluster stats {}".format(str(clusterName)))
            fetch_clusterstats()
        except Exception as e:
            logger.exception("Can't get cluster stats . Error {}. Emergency service halt".format(str(e)))
        try:
            logger.info("Get cluster nodes stats {}".format(str(clusterName)))            
            fetch_nodestats(clusterName)
        except Exception as e:
            logger.exception("Can't get cluster nodes stats . Error {}. Emergency service halt".format(str(e)))
        try:
            logger.info("Get cluster index stats {}".format(str(clusterName)))
            fetch_indexstats(clusterName)
        except Exception as e:
            logger.exception("Can't get cluster index stats . Error {}. Emergency service halt".format(str(e)))

        
        

##############
# Main Cycle #
##############

if __name__ == '__main__':

    # create logger 
    logger = logging.getLogger('elastic_metrics')
    logger.setLevel(logging.INFO)
    fh = RotatingFileHandler(logfilename, maxBytes=5 * 1024 * 1024, backupCount=5)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # get config. exit when error
    try:
        logger.info("Start working. Read config file")
        conf = yaml.load(open(config_file), Loader=yaml.SafeLoader)
    except Exception as e:
        logger.exception("Can't read config file {}. Error {}. Emergency service halt".format(str(config_file), str(e)))
        raise

    # start read config file
    try:
        elasticServer = conf.get("elasticServer",elasticServer)
        interval = conf.get("interval",interval)

        read_es_security_enable = conf.get("read_es_security_enable",read_es_security_enable)
        read_username = conf.get("read_username",read_username)
        read_password = conf.get("read_password",read_password)

        write_es_security_enable = conf.get("write_es_security_enable",write_es_security_enable)
        write_username = conf.get("write_username",write_username)
        write_password = conf.get("write_password",write_password)

        elasticIndex = conf.get("elasticIndex",elasticIndex)
        elasticMonitoringCluster = conf.get("elasticMonitoringCluster",elasticMonitoringCluster)
                
        certeficate = conf.get("certeficate",certeficate)

        logger.info("Read from config:\n\nelasticServer : {}\ninterval : {}\nelasticIndex : {}\nelasticMonitoringCluster : {}\n".format(str(elasticServer),str(interval),str(elasticIndex),str(elasticMonitoringCluster)))

    except Exception as e:
        logger.exception("Can't read needed parametrs from config file {}. Error {}. Emergency service halt".format(config_file,str(e)))
        raise
    # end read config file


    try:
        nextRun = 0
        while True:
            if time.time() >= nextRun:
                nextRun = time.time() + interval
                now = time.time()
                main()
                elapsed = time.time() - now
                #print("Total Elapsed Time: %s" % elapsed)
                logger.info("Total Elapsed Time: %s" % elapsed)
                timeDiff = nextRun - time.time()

                # Check timediff , if timediff >=0 sleep, if < 0 send metrics to es
                if timeDiff >= 0:
                    time.sleep(timeDiff)

    except KeyboardInterrupt:
        logger.info('Interrupted by keyboard')
        #print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
