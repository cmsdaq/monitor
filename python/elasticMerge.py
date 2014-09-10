#!/bin/env python2.6

import sys,traceback
import os
import datetime
import time

import logging
import threading

import csv
import socket

import requests
import simplejson as json
import ConfigParser

from utils import *

class elasticCollector:
	
	def __init__(self, inMonDir, inRunDir, rn):
		self.logger = logging.getLogger(self.__class__.__name__)
		self.inputMonDir = inMonDir

		self.insertedModuleLegend = False
		self.insertedPathLegend = False
		
		self.stoprequest = threading.Event()
		self.emptyQueue = threading.Event()
		self.source = False
		self.infile = False

	def stop(self):
		self.stoprequest.set()

	def run(self):
		self.logger.info("Start main loop")
		count = 0
		#while not (self.stoprequest.isSet() and self.emptyQueue.isSet()) :
		while not (self.stoprequest.isSet() and self.emptyQueue.isSet()) :
			before = dict ([(f, None) for f in os.listdir (self.inputMonDir)])
			time.sleep(1.0)
			after = dict ([(f, None) for f in os.listdir (self.inputMonDir)])
			self.logger.info(after)
			self.added = [f for f in after if not f in before]
			if self.added:
				self.logger.info('found file added')
				for f in self.added:
					self.logger.info('trying to make fileHandler for this file ' + f)
					try:
						self.infile = fileHandler(os.path.join(self.inputMonDir,f))
						self.emptyQueue.clear()
						self.logger.info('about to process file ' + os.path.join(self.inputMonDir,f))
						self.process() 
					except (KeyboardInterrupt) as e:
						self.emptyQueue.set()
					except (ValueError,IOError) as ex:
						self.logger.exception(ex)
			count+=1
		self.logger.info("Stop main loop")

	def process(self):
		self.logger.info("RECEIVED FILE: %s " %(self.infile.basename))
		self.logger.debug("RECEIVED FILE: %s " %(self.infile.basename))
		filepath = self.infile.filepath
		filetype = self.infile.filetype
		if filetype == OUTPUT or filetype == 7:
			self.logger.info("it's an output file")
			self.logger.info(self.infile.basename)
			es.elasticize_macromerge(self.infile)
			self.infile.deleteFile()
		else:
			self.logger.info(filetype)
class elasticMerge:
	def __init__(self,es_server_url,runnumber,startTime):
		self.logger = logging.getLogger(self.__class__.__name__)
		self.es_server_url=es_server_url
		self.index_name=conf.elastic_runindex_name
		self.runnumber = str(runnumber)
		self.startTime = startTime
		self.host = os.uname()[1]
		self.stopping=False
		self.threadEvent = threading.Event()
		self.mapping = {
			'macromerge' : {
				'_id'		:{'path':'id'},
				'_parent'	:{'type':'run'},
				'properties' : {
					'fm_date'	   :{'type':'date'},
					'id'			:{'type':'string'}, #run+appliance+stream+ls
					'appliance'	 :{'type':'string'},
					'stream'		:{'type':'string','index' : 'not_analyzed'},
					'ls'			:{'type':'integer'},
					'processed'	 :{'type':'integer'},
					'accepted'	  :{'type':'integer'},
					'errorEvents'   :{'type':'integer'},
					'size'		  :{'type':'integer'},
					}
				}
			}
		connectionAttempts=0
		while True:
			if self.stopping:break
			connectionAttempts+=1
			try:
				self.logger.info('writing to elastic index '+self.index_name)
				ip_url=getURLwithIP(es_server_url)
				
				
				# check that the central index exists
				check_index_response=requests.get(es_server_url+'/'+self.index_name+'/_stats')
				if check_index_response.status_code != 200:
					print 'elasticMerge: central index '+self.index_name+' does not exist or could not be found'
					self.logger.error('elasticMerge: central index '+self.index_name+' does not exist or could not be found')
					sys.exit(1)
				# check that the run document exists exists
				self.logger.info('querying "'+es_server_url+'/'+self.index_name+'/run/_search?q=runNumber:'+self.runnumber+'"')
			
				check_run_response=requests.get(es_server_url+'/'+self.index_name+'/run/_search?q=runNumber:'+self.runnumber)
				
				if check_run_response.status_code != 200:
					self.logger.info( check_run_response.raise_for_status())
					self.logger.error('elasticMerge: document for run # '+self.runnumber+' does not exist or could not be found (200 error)')
					sys.exit(1)
				elif check_run_response.json():
					if check_run_response.json()['hits']['total'] < 1:
						self.logger.error('elasticMerge: document for run # '+self.runnumber+' does not exist or could not be found')
						sys.exit(1)

				# create mapping in the central index for the macromerge using a PUT request
				respq=requests.put(es_server_url+'/'+self.index_name+'/macromerge/_mapping',data=json.dumps(self.mapping))
				
				# There could be a problem with a malformed POST request that causes an error other than cxn error
				# For now assume this does not happen as the request is entirely contained in this code
				# Otherwise should be handled similar to below
			
				#if respq.status_code != 200
				#	if runMode and connectionAttempts>100:
				#		self.logger.error('elasticMerge: exiting after 100 Elastic Http Error reports from '+ es_server_url)
				#		sys.exit(1)
				#	elif runMode==False and connectionAttempts>10:
				#		self.threadEvent.wait(60)
				#	else:
				#		self.threadEvent.wait(1)
				#else:
				#	break

				break
			except (requests.exceptions.ConnectionError,requests.exceptions.Timeout) as ex:
				#try to reconnect with different IP from DNS load balancing
				if runMode and connectionAttempts>100:
					self.logger.error('elasticMerge: exiting after 100 connection attempts to '+ es_server_url)
					sys.exit(1)
				elif runMode==False and connectionAttempts>10:
					self.threadEvent.wait(60)
				else:
					self.threadEvent.wait(1)
				continue
			
	def elasticize_macromerge(self,infile):
		basename = infile.basename
		self.logger.info(basename)
		data = infile.data['data']
		data.append(infile.mtime)
		data.append(infile.ls[2:])
		stream=infile.stream
		if stream.startswith("stream"): stream = stream[6:]
		data.append(stream)
		values=[]
		for f in data:
			if str(f).isdigit():
				values.append(int(f))
			else:
				values.append(str(f))
		values = [int(f) if str(f).isdigit() else str(f) for f in data]
		keys = ["processed","accepted","errorEvents","fname","size","eolField1","eolField2","fm_date","ls","stream"]
		document = dict(zip(keys, values))

		requests.post(self.es_server_url+'/_bulk','{"index": {"_parent": '+str(self.runnumber)+', "_type": "macromerge", "_index": "'+self.index_name+'"}}\n'+json.dumps(document)+'\n')

class monitorConf:
	def __init__(self,confFileName):
		cfg = ConfigParser.SafeConfigParser()
		cfg.read(confFileName)
		self.elastic_runindex_name = 'runindex'
		for sec in cfg.sections():
			for item,value in cfg.items(sec):
				self.__dict__[item] = value
		self.run_number_padding = int(self.run_number_padding)



#Output redirection class
class stdOutLog:
	def __init__(self):
		self.logger = logging.getLogger(self.__class__.__name__)	
	def write(self, message):
		self.logger.debug(message)
class stdErrorLog:
	def __init__(self):
		self.logger = logging.getLogger(self.__class__.__name__)
	def write(self, message):
		self.logger.error(message)

if __name__ == "__main__":
	confFileName=os.path.join(sys.path[0],'../config.txt');
	print 'loaded config from `' + confFileName + '`'
	conf=monitorConf(confFileName)
	print 'connecting to ES server: '+conf.es_server_url

	try:
		os.makedirs(conf.log_dir)
	except OSError as ex:
		pass

	logging.basicConfig(filename=os.path.join(conf.log_dir,"elasticMerge_py.log"),
					level=logging.INFO,
					format='%(levelname)s:%(asctime)s - %(funcName)s - %(message)s',
					datefmt='%Y-%m-%d %H:%M:%S')
	logger = logging.getLogger(os.path.basename(__file__))

	#STDOUT AND ERR REDIRECTIONS
	sys.stderr = stdErrorLog()
	sys.stdout = stdOutLog()

	#es_server = sys.argv[0]
	#dirname = sys.argv[1]
	dirname=conf.dirname
	#outputdir = sys.argv[2]
	#runnumber = sys.argv[3]
	runnumber = sys.argv[1]
	dt=os.path.getctime(dirname)
	startTime = datetime.datetime.utcfromtimestamp(dt).isoformat()
	
	#EoR file path to watch for

	monDir = os.path.join(dirname,"mon")
	logger.info("starting elastic for "+monDir)

	try:
		logger.info("try create mon dir " + monDir)
		os.makedirs(monDir)
	except OSError as ex:
		logger.info(ex)
		pass
	mr = None
	try:
		es = elasticMerge(conf.es_server_url,runnumber,startTime)
		#starting elasticCollector thread
		ec = elasticCollector(monDir,dirname, runnumber.zfill(conf.run_number_padding))
		ec.run()

	except Exception as e:
		logger.exception(e)
		print traceback.format_exc()
		logger.error("when processing files from directory "+monDir)

	logging.info("Quit")
	sys.exit(0)

