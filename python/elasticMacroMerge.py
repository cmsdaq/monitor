#!/bin/env python

import sys,traceback
import os
import datetime
import time

import logging
import threading
import Queue

import csv
import socket

import requests
import simplejson as json
import ConfigParser

def getURLwithIP(url):
  try:
      prefix = ''
      if url.startswith('http://'):
          prefix='http://'
          url = url[7:]
      suffix=''
      port_pos=url.rfind(':')
      if port_pos!=-1:
          suffix=url[port_pos:]
          url = url[:port_pos]
  except Exception as ex:
      logging.error('could not parse URL ' +url)
      raise(ex)
  if url!='localhost':
      ip = socket.gethostbyname(url)
  else: ip='127.0.0.1'

  return prefix+str(ip)+suffix


#class elasticBandBU:
#
#    def __init__(self,es_server_url,runnumber,startTime,runMode=True):
#        self.logger = logging.getLogger(self.__class__.__name__)
#        self.es_server_url=es_server_url
#        self.index_name=conf.elastic_runindex_name
#        self.runnumber = str(runnumber)
#        self.startTime = startTime
#        self.host = os.uname()[1]
#        self.stopping=False
#        self.threadEvent = threading.Event()
#        self.runMode=runMode
#        self.settings = {
#            "analysis":{
#                "analyzer": {
#                    "prefix-test-analyzer": {
#                        "type": "custom",
#                        "tokenizer": "prefix-test-tokenizer"
#                    }
#                },
#                "tokenizer": {
#                    "prefix-test-tokenizer": {
#                        "type": "path_hierarchy",
#                        "delimiter": " "
#                    }
#                }
#             },
#            "index":{
#                'number_of_shards' : 10,
#                'number_of_replicas' : 3
#            },
#        }
#
#        self.run_mapping = {
#            'run' : {
##                '_routing' :{
##                    'required' : True,
##                    'path'     : 'runNumber'
##                },
#                '_id' : {
#                    'path' : 'runNumber'
#                },
#                'properties' : {
#                    'runNumber':{
#                        'type':'integer'
#                        },
#                    'startTimeRC':{
#                        'type':'date'
#                            },
#                    'stopTimeRC':{
#                        'type':'date'
#                            },
#                    'startTime':{
#                        'type':'date'
#                            },
#                    'endTime':{
#                        'type':'date'
#                            },
#                    'completedTime' : {
#                        'type':'date'
#                            }
#                },
#                '_timestamp' : {
#                    'enabled' : True,
#                    'store'   : 'yes'
#                    }
#            },
#            'microstatelegend' : {
#
#                '_id' : {
#                    'path' : 'id'
#                },
#                '_parent':{'type':'run'},
#                'properties' : {
#                    'names':{
#                        'type':'string'
#                        },
#                    'id':{
#                        'type':'string'
#                        }
#                    }
#            },
#            'pathlegend' : {
#
#                '_id' : {
#                    'path' : 'id'
#                },
#                '_parent':{'type':'run'},
#                'properties' : {
#                    'names':{
#                        'type':'string'
#                        },
#                    'id':{
#                        'type':'string'
#                        }
#
#                    }
#                },
#            'boxinfo' : {
#                '_id'        :{'path':'id'},#TODO:remove
#                'properties' : {
#                    'fm_date'       :{'type':'date'},
#                    'id'            :{'type':'string'},
#                    'broken'        :{'type':'integer'},
#                    'used'          :{'type':'integer'},
#                    'idles'         :{'type':'integer'},
#                    'quarantined'   :{'type':'integer'},
#                    'usedDataDir'   :{'type':'integer'},
#                    'totalDataDir'  :{'type':'integer'},
#                    'usedRamdisk'   :{'type':'integer'},
#                    'totalRamdisk'  :{'type':'integer'},
#                    'usedOutput'    :{'type':'integer'},
#                    'totalOutput'   :{'type':'integer'},
#                    'activeRuns'    :{'type':'string'}
#                    },
#                '_timestamp' : { 
#                    'enabled'   : True,
#                    'store'     : "yes",
#                    "path"      : "fm_date"
#                    },
#                '_ttl'       : { 'enabled' : True,
#                              'default' :  '30d'
#                    }
#                },
#
#            'boxinfo_last' : {
#                '_id'        :{'path':'id'},
#                'properties' : {
#                    'fm_date'       :{'type':'date'},
#                    'id'            :{'type':'string'},
#                    'broken'        :{'type':'integer'},
#                    'used'          :{'type':'integer'},
#                    'idles'         :{'type':'integer'},
#                    'quarantined'   :{'type':'integer'},
#                    'usedDataDir'   :{'type':'integer'},
#                    'totalDataDir'  :{'type':'integer'},
#                    'usedRamdisk'   :{'type':'integer'},
#                    'totalRamdisk'  :{'type':'integer'},
#                    'usedOutput'    :{'type':'integer'},
#                    'totalOutput'   :{'type':'integer'},
#                    'activeRuns'    :{'type':'string'}
#                    },
#                '_timestamp' : { 
#                    'enabled'   : True,
#                    'store'     : "yes",
#                    "path"      : "fm_date"
#                    }
#                },
#
#            'eols' : {
#                '_id'        :{'path':'id'},
#                '_parent'    :{'type':'run'},
#                'properties' : {
#                    'fm_date'       :{'type':'date'},
#                    'id'            :{'type':'string'},
#                    'ls'            :{'type':'integer'},
#                    'NEvents'       :{'type':'integer'},
#                    'NFiles'        :{'type':'integer'},
#                    'TotalEvents'   :{'type':'integer'}
#                    },
#                '_timestamp' : { 
#                    'enabled'   : True,
#                    'store'     : "yes",
#                    "path"      : "fm_date"
#                    },
#                },
#            'minimerge' : {
#                '_id'        :{'path':'id'},
#                '_parent'    :{'type':'run'},
#                'properties' : {
#                    'fm_date'       :{'type':'date'},
#                    'id'            :{'type':'string'}, #run+appliance+stream+ls
#                    'appliance'     :{'type':'string'},
#                    'stream'        :{'type':'string','index' : 'not_analyzed'},
#                    'ls'            :{'type':'integer'},
#                    'processed'     :{'type':'integer'},
#                    'accepted'      :{'type':'integer'},
#                    'errorEvents'   :{'type':'integer'},
#                    'size'          :{'type':'integer'},
#                    }
#                }
#            }
#
#
#        connectionAttempts=0
#        while True:
#            if self.stopping:break
#            connectionAttempts+=1
#            try:
#                self.logger.info('writing to elastic index '+self.index_name)
#                ip_url=getURLwithIP(es_server_url)
#                self.es = ElasticSearch(es_server_url)
#                self.es.create_index(self.index_name, settings={ 'settings': self.settings, 'mappings': self.run_mapping })
#                break
#            except ElasticHttpError as ex:
#                #this is normally fine as the index gets created somewhere across the cluster
#                if "IndexAlreadyExistsException" in str(ex):
#                    self.logger.info(ex)
#                    break
#                if "InvalidIndexNameException" in str(ex):
#                    self.logger.info(ex)
#                    break
#                else:
#                    self.logger.error(ex)
#                    if runMode and connectionAttempts>100:
#                        self.logger.error('elastic (BU): exiting after 100 ElasticHttpError reports from '+ es_server_url)
#                        sys.exit(1)
#                    elif runMode==False and connectionAttempts>10:
#                        self.threadEvent.wait(60)
#                    else:
#                        self.threadEvent.wait(1)
#                    continue
#
#            except (ConnectionError,Timeout) as ex:
#                #try to reconnect with different IP from DNS load balancing
#                if runMode and connectionAttempts>100:
#                   self.logger.error('elastic (BU): exiting after 100 connection attempts to '+ es_server_url)
#                   sys.exit(1)
#                elif runMode==False and connectionAttempts>10:
#                   self.threadEvent.wait(60)
#                else:
#                   self.threadEvent.wait(1)
#                continue
#            
#        #write run number document
#        if runMode == True:
#            document = {}
#            document['runNumber'] = self.runnumber
#            document['startTime'] = startTime
#            documents = [document]
#            self.index_documents('run',documents)
#            #except ElasticHttpError as ex:
#            #    self.logger.info(ex)
#            #    pass
#
#    def resetURL(url):
#        self.es = None
#        self.es = ElasticSearch(url)
#
#    def read_line(self,fullpath):
#        with open(fullpath,'r') as fp:
#            return fp.readline()
#    
#    def elasticize_modulelegend(self,fullpath):
#
#        self.logger.info(os.path.basename(fullpath))
#        stub = self.read_line(fullpath)
#        document = {}
#        document['_parent']= self.runnumber
#        document['id']= "microstatelegend_"+self.runnumber
#        document['names']= self.read_line(fullpath)
#        documents = [document]
#        return self.index_documents('microstatelegend',documents)
#
#
#    def elasticize_pathlegend(self,fullpath):
#
#        self.logger.info(os.path.basename(fullpath))
#        stub = self.read_line(fullpath)
#        document = {}
#        document['_parent']= self.runnumber
#        document['id']= "pathlegend_"+self.runnumber
#        document['names']= self.read_line(fullpath)
#        documents = [document]
#        return self.index_documents('pathlegend',documents)
#
#    def elasticize_runend_time(self,endtime):
#
#        self.logger.info(str(endtime)+" going into buffer")
#        document = {}
#        document['runNumber'] = self.runnumber
#        document['startTime'] = self.startTime
#        document['endTime'] = endtime
#        documents = [document]
#        self.index_documents('run',documents)
#
#    def elasticize_box(self,infile):
#
#        basename = infile.basename
#        self.logger.debug(basename)
#        try:
#            document = infile.data
#            #TODO:let dynamic ID
#            document['id']= basename + '_' + document['fm_date'].split('.')[0] #TODO:remove
#            documents = [document]
#        except:
#            #in case of malformed box info
#            return
#        self.index_documents('boxinfo',documents)
#        #self.logger.info(str(document))#check that ID is not present...
#        #TODO:write unique boxinfo
#        #documents[0]['id']=basename
#        #self.index_documents('boxinfo_last',documents)
#
#    def elasticize_eols(self,infile):
#        basename = infile.basename
#        self.logger.info(basename)
#        data = infile.data['data']
#        data.append(infile.mtime)
#        data.append(infile.ls[2:])
#        
#        values = [int(f) if f.isdigit() else str(f) for f in data]
#        keys = ["NEvents","NFiles","TotalEvents","fm_date","ls"]
#        document = dict(zip(keys, values))
#
#        document['id'] = infile.name+"_"+os.uname()[1]
#        document['_parent']= self.runnumber
#        documents = [document]
#        self.index_documents('eols',documents)
#
#    def elasticize_minimerge(self,infile):
#        basename = infile.basename
#        self.logger.info(basename)
#        data = infile.data['data']
#        data.append(infile.mtime)
#        data.append(infile.ls[2:])
#        stream=infile.stream
#        if stream.startswith("stream"): stream = stream[6:]
#        data.append(stream)
#        values = [int(f) if str(f).isdigit() else str(f) for f in data]
#        keys = ["processed","accepted","errorEvents","fname","size","eolField1","eolField2","fm_date","ls","stream"]
#        document = dict(zip(keys, values))
#        document['id'] = infile.name
#        document['_parent']= self.runnumber
#        documents = [document]
#        self.index_documents('minimerge',documents)
#
#    def index_documents(self,name,documents):
#        attempts=0
#        while True:
#            attempts+=1
#            try:
#                self.es.bulk_index(self.index_name,name,documents)
#                return True
#            except ElasticHttpError as ex:
#                if attempts<=1:continue
#                self.logger.error('elasticsearch HTTP error. skipping document '+name)
#                #self.logger.exception(ex)
#                return False
#            except (ConnectionError,Timeout) as ex:
#                if attempts>100 and self.runMode:
#                    raise(ex)
#                self.logger.error('elasticsearch connection error. retry.')
#                if self.stopping:return False
#                time.sleep(0.1)
#                ip_url=getURLwithIP(self.es_server_url)
#                self.es = ElasticSearch(ip_url)
#        return False
#             
#
#class elasticCollectorBU():
#
#    
#    def __init__(self, inMonDir, inRunDir, watchdir, rn):
#        self.logger = logging.getLogger(self.__class__.__name__)
#        self.inputMonDir = inMonDir
#        
#
#        self.insertedModuleLegend = False
#        self.insertedPathLegend = False
#        self.eorCheckPath = inRunDir + '/run' +  str(rn).zfill(conf.run_number_padding) + '_ls0000_EoR.jsn'
#        
#        self.stoprequest = threading.Event()
#        self.emptyQueue = threading.Event()
#        self.source = False
#        self.infile = False
#
#    def start(self):
#        self.run()
#
#    def stop(self):
#        self.stoprequest.set()
#
#    def run(self):
#        self.logger.info("Start main loop")
#        count = 0
#        while not (self.stoprequest.isSet() and self.emptyQueue.isSet()) :
#            if self.source:
#                try:
#                    event = self.source.get(True,1.0) #blocking with timeout
#                    self.eventtype = event.mask
#                    self.infile = fileHandler(event.fullpath)
#                    self.emptyQueue.clear()
#                    self.process() 
#                except (KeyboardInterrupt,Queue.Empty) as e:
#                    self.emptyQueue.set()
#                except (ValueError,IOError) as ex:
#                    self.logger.exception(ex)
#            else:
#                time.sleep(1.0)
#            #check for EoR file every 5 intervals
#            count+=1
#            if (count%5) == 0:
#                if os.path.exists(self.eorCheckPath):
#                    if es:
#                        dt=os.path.getctime(self.eorCheckPath)
#                        endtime = datetime.datetime.utcfromtimestamp(dt).isoformat()
#                        es.elasticize_runend_time(endtime)
#                    break
#                if False==os.path.exists(self.eorCheckPath[:self.eorCheckPath.rfind('/')]):
#                    #run dir deleted
#                    break
#        self.logger.info("Stop main loop")
#
#
#    def setSource(self,source):
#        self.source = source
#
#
#    def process(self):
#        self.logger.debug("RECEIVED FILE: %s " %(self.infile.basename))
#        filepath = self.infile.filepath
#        filetype = self.infile.filetype
#        eventtype = self.eventtype
#        if es and eventtype & (inotify.IN_CLOSE_WRITE | inotify.IN_MOVED_TO):
#            if filetype in [MODULELEGEND] and self.insertedModuleLegend == False:
#                if es.elasticize_modulelegend(filepath):
#                    self.insertedModuleLegend = True
#            elif filetype in [PATHLEGEND] and self.insertedPathLegend == False:
#                if es.elasticize_pathlegend(filepath):
#                    self.insertedPathLegend = True
#            elif filetype == EOLS:
#                self.logger.info(self.infile.basename)
#                es.elasticize_eols(self.infile)
#            elif filetype == OUTPUT:
#                #mini-merged json file on BU
#                self.logger.info(self.infile.basename)
#                es.elasticize_minimerge(self.infile)
#                self.infile.deleteFile()
#
#
#class elasticBoxCollectorBU():
#
#    def __init__(self,esbox):
#        self.logger = logging.getLogger(self.__class__.__name__)
#        
#        self.stoprequest = threading.Event()
#        self.emptyQueue = threading.Event()
#        self.source = False
#        self.infile = False
#        self.es = esbox
#
#    def start(self):
#        self.run()
#
#    def stop(self):
#        self.stoprequest.set()
#
#    def run(self):
#        self.logger.info("Start main loop")
#        while not (self.stoprequest.isSet() and self.emptyQueue.isSet()) :
#            if self.source:
#                try:
#                    event = self.source.get(True,1.0) #blocking with timeout
#                    self.eventtype = event.mask
#                    self.infile = fileHandler(event.fullpath)
#                    self.emptyQueue.clear()
#                    self.process() 
#                except (KeyboardInterrupt,Queue.Empty) as e:
#                    self.emptyQueue.set()
#                except (ValueError,IOError) as ex:
#                    self.logger.exception(ex)
#            else:
#                time.sleep(1.0)
#        self.logger.info("Stop main loop")
#
#    def setSource(self,source):
#        self.source = source
#
#    def process(self):
#        self.logger.debug("RECEIVED FILE: %s " %(self.infile.basename))
#        filepath = self.infile.filepath
#        filetype = self.infile.filetype
#        eventtype = self.eventtype
#        if filetype == BOX:
#            #self.logger.info(self.infile.basename)
#            self.es.elasticize_box(self.infile)
#
#
#class BoxInfoUpdater(threading.Thread):
#
#    def __init__(self,ramdisk):
#        self.logger = logging.getLogger(self.__class__.__name__)
#        self.stopping = False
#
#        try:
#            threading.Thread.__init__(self)
#            self.threadEvent = threading.Event()
#
#            boxesDir =  os.path.join(ramdisk,'appliance/boxes')
#            boxesMask = inotify.IN_CLOSE_WRITE 
#            self.logger.info("starting elastic for "+boxesDir)
#
#            try:
#                os.makedirs(boxesDir)
#            except:
#                pass
#    
#            self.eventQueue = Queue.Queue()
#            self.mr = MonitorRanger()
#            self.mr.setEventQueue(self.eventQueue)
#            self.mr.register_inotify_path(boxesDir,boxesMask)
#
#        except Exception,ex:
#            self.logger.exception(ex)
#
#    def run(self):
#        try:
#            self.es = elasticBandBU(conf.elastic_runindex_url,0,'',False)
#            if self.stopping:return
#
#            self.ec = elasticBoxCollectorBU(self.es)
#            self.ec.setSource(self.eventQueue)
#
#            self.mr.start_inotify()
#            self.ec.start()
#        except Exception,ex:
#            self.logger.exception(ex)
#
#    def stop(self):
#        try:
#            self.stopping=True
#            self.threadEvent.set()
#            if self.es:
#                self.es.stopping=True
#                self.es.threadEvent.set()
#            if self.mr is not None:
#                self.mr.stop_inotify()
#            if self.ec is not None:
#                self.ec.stop()
#            self.join()
#        except RuntimeError,ex:
#            pass
#        except Exception,ex:
#            self.logger.exception(ex)
#
#class RunCompletedChecker(threading.Thread):
#
#    def __init__(self,mode,nr,nresources,run_dir,active_runs):
#        self.logger = logging.getLogger(self.__class__.__name__)
#        self.mode = mode
#        self.nr = nr
#        self.nresources = nresources
#        self.rundirCheckPath = conf.watch_directory +'/run'+ str(nr).zfill(conf.run_number_padding)
#        self.eorCheckPath = os.path.join(self.rundirCheckPath,'run' +  str(nr).zfill(conf.run_number_padding) + '_ls0000_EoR.jsn')
#        self.url = 'http://localhost:9200/run'+str(nr).zfill(conf.run_number_padding)+'*/fu-complete/_count'
#        self.urlclose = 'http://localhost:9200/run'+str(nr).zfill(conf.run_number_padding)+'*/_close'
#        self.logurlclose = 'http://localhost:9200/log_run'+str(nr).zfill(conf.run_number_padding)+'*/_close'
#
#        self.urlsearch = 'http://localhost:9200/run'+str(nr)+'*/fu-complete/_search?size=1'
#        self.url_query = '{  "query": { "filtered": {"query": {"match_all": {}}}}, "sort": { "fm_date": { "order": "desc" }}}'
#
#
#        self.stop = False
#        self.threadEvent = threading.Event()
#        self.run_dir = run_dir
#        self.active_runs = active_runs
#        try:
#            threading.Thread.__init__(self)
#
#        except Exception,ex:
#            self.logger.exception(ex)
#
#
#    def checkBoxes(self,dir):
#
#
#        files = os.listdir(dir)
#        endAllowed=True
#        runFound=False
#        for file in files:
#            if file != os.uname()[1]:
#                #ignore file if it is too old (FU with a problem)
#                if time.time() - os.path.getmtime(dir+file) > 20:continue
#
#                f = open(dir+file,'r')
#                lines = f.readlines()
#                #test that we are not reading incomplete file
#                try:
#                    if lines[-1].startswith('entriesComplete'):pass
#                    else:
#                        endAllowed=False
#                        break
#                except:
#                    endAllowed=False
#                    break
#                firstCopy=None
#                for l in lines:
#                    if l.startswith('activeRuns='):
#                        if firstCopy==None:
#                            firstCopy=l
#                            continue
#                        else:
#                            if firstCopy!=l:
#                                endAllowed=False
#                                break
#                        runstring = l.split('=')
#                        try:
#                            runs = runstring[1].strip('\n ').split(',')
#                            for run in runs:
#                                if run.isdigit()==False:continue
#                                if int(run)==int(self.nr):
#                                    runFound=True
#                                    break
#                        except:
#                            endAllowed=False
#                        break
#                if firstCopy==None:endAllowed=False
#                if runFound==True:break
#                if endAllowed==False:break
#        if endAllowed==True and runFound==False: return False
#        else:return True
#
#
#    def run(self):
#
#        self.threadEvent.wait(10)
#        while self.stop == False:
#            self.threadEvent.wait(5)
#            if self.stop: return#giving up
#            if os.path.exists(self.eorCheckPath) or os.path.exists(self.rundirCheckPath)==False:
#                break
#
#        dir = conf.resource_base+'/boxes/'
#        check_boxes=True
#        check_es_complete=True
#        total_es_elapsed=0
#
#        while self.stop==False:
#            if check_boxes:
#                check_boxes = self.checkBoxes(dir)
#
#            if check_boxes==False:
#                try:
#                    self.active_runs.remove(int(self.nr))
#                except:pass
#
#            if check_es_complete:
#                try:
#                    resp = requests.post(self.url, '')
#                    data = json.loads(resp.content)
#                    if int(data['count']) >= len(self.nresources):
#                        try:
#                            respq = requests.post(self.urlsearch,self.url_query)
#                            dataq = json.loads(respq.content)
#                            fm_time = str(dataq['hits']['hits'][0]['_source']['fm_date'])
#                            #fill in central index completition time
#                            #EXPERIMENTAL!
#                            postq = "{runNumber\":\"" + str(self.nr) + "\",\"completedTime\" : \"" + fm_time + "\"}"
#                            requests.post(conf.es_runindex_url+'/'+conf.es_runindex_name+'/run',putq)
#                            self.logger.info("filled in completition time for run"+str(self.nr))
#                        except Exception as ex:
#                            self.logger.exception(ex)
#                        try:
#                            if conf.close_es_index==True:
#                                #wait a bit for central ES queries to complete
#                                time.sleep(10)
#                                resp = requests.post(self.urlclose)
#                                self.logger.info('closed appliance ES index for run '+str(self.nr))
#                        except Exception as exc:
#                            self.logger.error('Error in run completition check')
#                            self.logger.exception(exc)
#                        check_es_complete=False
#                        continue
#                    else:
#                        time.sleep(5)
#                        total_es_elapsed+=5
#                        if total_es_elapsed>600:
#                            self.logger.error('run index complete flag was not written by all FUs, giving up after 10 minutes.')
#                            check_es_complete=False
#                            continue
#                except Exception,ex:
#                    self.logger.error('Error in run completition check')
#                    self.logger.exception(ex)
#                    check_es_complete=False
#
#            #exit fi both checks are complete
#            if check_boxes==False and check_es_complete==False:return
#            #check every 10 seconds
#            self.threadEvent.wait(10)
#
#    def stop(self):
#        self.stop = True
#        self.threadEvent.set() 
#
#
#
class monitorConf:
	def __init__(self,confFileName):
		cfg = ConfigParser.SafeConfigParser()
		cfg.read(confFileName)
		self.elastic_runindex_name = 'runindex'
		for sec in cfg.sections():
			for item,value in cfg.items(sec):
				self.__dict__[item] = value



if __name__ == "__main__":
	print "sys.path[0]: "+sys.path[0]
	confFileName=os.path.join(sys.path[0],'../config.txt');
	conf=monitorConf(confFileName)


#    logging.basicConfig(filename=os.path.join(conf.log_dir,"elasticbu.log"),
#                    level=logging.INFO,
#                    format='%(levelname)s:%(asctime)s - %(funcName)s - %(message)s',
#                    datefmt='%Y-%m-%d %H:%M:%S')
#    logger = logging.getLogger(os.path.basename(__file__))
#
#    #STDOUT AND ERR REDIRECTIONS
#    sys.stderr = stdErrorLog()
#    sys.stdout = stdOutLog()
#
#    eventQueue = Queue.Queue()
#
#    es_server = sys.argv[0]
#    dirname = sys.argv[1]
#    outputdir = sys.argv[2]
#    runnumber = sys.argv[3]
#    watchdir = conf.watch_directory
#    dt=os.path.getctime(dirname)
#    startTime = datetime.datetime.utcfromtimestamp(dt).isoformat()
#    
#    #EoR file path to watch for
#
#    mainDir = dirname
#    mainMask = inotify.IN_CLOSE_WRITE |  inotify.IN_MOVED_TO
#    monDir = os.path.join(dirname,"mon")
#    monMask = inotify.IN_CLOSE_WRITE |  inotify.IN_MOVED_TO
#    outMonDir = os.path.join(outputdir,"mon")
#    outMonMask = inotify.IN_CLOSE_WRITE |  inotify.IN_MOVED_TO
#
#    logger.info("starting elastic for "+mainDir)
#    logger.info("starting elastic for "+monDir)
#
#    try:
#        logger.info("try create input mon dir " + monDir)
#        os.makedirs(monDir)
#    except OSError,ex:
#        logger.info(ex)
#        pass
#
#    try:
#        logger.info("try create output mon dir " + outMonDir)
#        os.makedirs(outMonDir)
#    except OSError,ex:
#        logger.info(ex)
#        pass
#
#    mr = None
#    try:
#        #starting inotify thread
#        mr = MonitorRanger()
#        mr.setEventQueue(eventQueue)
#        mr.register_inotify_path(monDir,monMask)
#        mr.register_inotify_path(outMonDir,outMonMask)
#        mr.register_inotify_path(mainDir,mainMask)
#
#        mr.start_inotify()
#
#        es = elasticBandBU(conf.elastic_runindex_url,runnumber,startTime)
#
#        #starting elasticCollector thread
#        ec = elasticCollectorBU(monDir,dirname, watchdir, runnumber.zfill(conf.run_number_padding))
#        ec.setSource(eventQueue)
#        ec.start()
#
#    except Exception as e:
#        logger.exception(e)
#        print traceback.format_exc()
#        logger.error("when processing files from directory "+monDir)
#
#    logging.info("Closing notifier")
#    if mr is not None:
#      mr.stop_inotify()
#
#    logging.info("Quit")
#    sys.exit(0)
#
