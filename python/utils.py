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

ES_DIR_NAME = "TEMP_ES_DIRECTORY"
UNKNOWN,JSD,STREAM,INDEX,FAST,SLOW,OUTPUT,STREAMERR,STREAMDQMHISTOUTPUT,INI,EOLS,EOR,COMPLETE,DAT,PDAT,PIDPB,PB,CRASH,MODULELEGEND,PATHLEGEND,BOX,BOLS = range(22)            #file types 
TO_ELASTICIZE = [STREAM,INDEX,OUTPUT,STREAMERR,STREAMDQMHISTOUTPUT,EOLS,EOR,COMPLETE]
TEMPEXT = ".recv"
ZEROLS = 'ls0000'
STREAMERRORNAME = 'streamError'
STREAMDQMHISTNAME = 'streamDQMHistograms'



class fileHandler(object):
    def __eq__(self,other):
        return self.filepath == other.filepath

    def __getattr__(self,name):
        if name not in self.__dict__: 
            if name in ["dir","ext","basename","name"]: self.getFileInfo() 
            elif name in ["filetype"]: self.filetype = self.getFiletype();
            elif name in ["run","ls","stream","index","pid"]: self.getFileHeaders()
            elif name in ["data"]: self.data = self.getData(); 
            elif name in ["definitions"]: self.getDefinitions()
            elif name in ["host"]: self.host = os.uname()[1];
        if name in ["ctime"]: self.ctime = self.getTime('c')
        if name in ["mtime"]: self.mtime = self.getTime('m')
        return self.__dict__[name]

    def __init__(self,filepath):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.filepath = filepath
        self.outDir = self.dir
        self.mergeStage = 0
        self.inputs = []

    def getTime(self,t):
        if self.exists():
            if t == 'c':
                dt=os.path.getctime(self.filepath)
            elif t == 'm':
                dt=os.path.getmtime(self.filepath)
            time = datetime.datetime.utcfromtimestamp(dt).isoformat() 
            return time
        return None   
                
    def getFileInfo(self):
        self.dir = os.path.dirname(self.filepath)
        self.basename = os.path.basename(self.filepath)
        self.name,self.ext = os.path.splitext(self.basename)

    def getFiletype(self,filepath = None):
        if not filepath: filepath = self.filepath
        filename = self.basename
        name,ext = self.name,self.ext
        name = name.upper()
        if "mon" not in filepath:
            if ext == ".dat" and "_PID" not in name: return DAT
            if ext == ".dat" and "_PID" in name: return PDAT
            if ext == ".ini" and "_PID" in name: return INI
            if ext == ".jsd" and "OUTPUT" in name: return JSD
            if ext == ".jsn":
                if STREAMERRORNAME.upper() in name: return STREAMERR
                elif "BOLS" in name : return BOLS
                elif "STREAM" in name and "_PID" in name: return STREAM
                elif "INDEX" in name and  "_PID" in name: return INDEX
                elif "CRASH" in name and "_PID" in name: return CRASH
                elif "EOLS" in name: return EOLS
                elif "EOR" in name: return EOR
        if ext==".jsn":
            if STREAMDQMHISTNAME.upper() in name and "_PID" not in name: return STREAMDQMHISTOUTPUT
            if "STREAM" in name and "_PID" not in name: return OUTPUT
        if ext==".pb":
            if "_PID" not in name: return PB
            else: return PIDPB
        if name.endswith("COMPLETE"): return COMPLETE
        if ".fast" in filename: return FAST
        if "slow" in filename: return SLOW
        if ext == ".leg" and "MICROSTATELEGEND" in name: return MODULELEGEND
        if ext == ".leg" and "PATHLEGEND" in name: return PATHLEGEND
        if "boxes" in filepath : return BOX
        return UNKNOWN


    def getFileHeaders(self):
        filetype = self.filetype
        name,ext = self.name,self.ext
        splitname = name.split("_")
        if filetype in [STREAM,INI,PDAT,PIDPB,CRASH]: self.run,self.ls,self.stream,self.pid = splitname
        elif filetype == SLOW: self.run,self.ls,self.pid = splitname
        elif filetype == FAST: self.run,self.pid = splitname
        elif filetype in [DAT,PB,OUTPUT,STREAMERR,STREAMDQMHISTOUTPUT]: self.run,self.ls,self.stream,self.host = splitname
        elif filetype == INDEX: self.run,self.ls,self.index,self.pid = splitname
        elif filetype == EOLS: self.run,self.ls,self.eols = splitname
        else: 
			print 'Bad filetype: '+self.filepath
			self.logger.warning("Bad filetype: %s" %self.filepath)
			self.run,self.ls,self.stream = [None]*3

    def getData(self):
        if self.ext == '.jsn': return self.getJsonData()
        elif self.filetype == BOX: return self.getBoxData()
        return None

    def getBoxData(self,filepath = None):
        if not filepath: filepath = self.filepath
        sep = '\n'
        try:
            with open(filepath,'r') as fi:
                data = fi.read()
                data = data.strip(sep).split(sep)
                data = dict([d.split('=') for d in data])
        except StandardError,e:
            self.logger.exception(e)
            data = {}


        return data

        #get data from json file
    def getJsonData(self,filepath = None):
        if not filepath: filepath = self.filepath
        try:
            with open(filepath) as fi:
                data = json.load(fi)
        except StandardError,e:
            self.logger.exception(e)
            data = {}
        except json.scanner.JSONDecodeError,e:
            self.logger.exception(e)
            data = None
        return data

    def setJsdfile(self,jsdfile):
        self.jsdfile = jsdfile
        if self.filetype in [OUTPUT,STREAMDQMHISTOUTPUT,CRASH,STREAMERR]: self.initData()
        
    def initData(self):
        defs = self.definitions
        self.data = {}
        if defs:
            self.data["data"] = [self.nullValue(f["type"]) for f in defs]

    def nullValue(self,ftype):
        if ftype == "integer": return "0"
        elif ftype  == "string": return ""
        else: 
            self.logger.warning("bad field type %r" %(ftype))
            return "ERR"

    def checkSources(self):
        data,defs = self.data,self.definitions
        for item in defs:
            fieldName = item["name"]
            index = defs.index(item)
            if "source" in item: 
                source = item["source"]
                sIndex,ftype = self.getFieldIndex(field)
                data[index] = data[sIndex]

    def getFieldIndex(self,field):
        defs = self.definitions
        if defs: 
            index = next((defs.index(item) for item in defs if item["name"] == field),-1)
            ftype = defs[index]["type"]
            return index,ftype

        
    def getFieldByName(self,field):
        index,ftype = self.getFieldIndex(field)
        data = self.data["data"]
        if index > -1:
            value = int(data[index]) if ftype == "integer" else str(data[index]) 
            return value
        else:
            self.logger.warning("bad field request %r in %r" %(field,self.definitions))
            return False

    def setFieldByName(self,field,value):
        index,ftype = self.getFieldIndex(field)
        data = self.data["data"]
        if index > -1:
            data[index] = value
            return True
        else:
            self.logger.warning("bad field request %r in %r" %(field,self.definitions))
            return False

        #get definitions from jsd file
    def getDefinitions(self):
        if self.filetype == STREAM:
            self.jsdfile = self.data["definition"]
        elif not self.jsdfile: 
            self.logger.warning("jsd file not set")
            self.definitions = {}
            return False
        self.definitions = self.getJsonData(self.jsdfile)["data"]
        return True


    def deleteFile(self):
        #return True
        filepath = self.filepath
        self.logger.info(filepath)
        if os.path.isfile(filepath):
            try:
                os.remove(filepath)
            except Exception,e:
                self.logger.exception(e)
                return False
        return True

    def moveFile(self,newpath,copy = False):
        if not self.exists(): return True
        oldpath = self.filepath
        newdir = os.path.dirname(newpath)

        if not os.path.exists(oldpath): return False

        self.logger.info("%s -> %s" %(oldpath,newpath))
        retries = 5
        newpath_tmp = newpath+TEMPEXT
        while True:
          try:
              if not os.path.isdir(newdir): os.makedirs(newdir)
              if copy: shutil.copy(oldpath,newpath_tmp)
              else: 
                  shutil.move(oldpath,newpath_tmp)
              break

          except (OSError,IOError),e:
              self.logger.exception(e)
              retries-=1
              if retries == 0:
                  self.logger.error("Failure to move file "+str(oldpath)+" to "+str(newpath_tmp))
                  return False
              else:
                  time.sleep(0.5)
        retries = 5
        while True:
        #renaming
            try:
                #shutil.move(newpath,newpath.replace(TEMPEXT,""))
                os.rename(newpath_tmp,newpath)
                break
            except (OSError,IOError),e:
                self.logger.exception(e)
                retries-=1
                if retries == 0:
                    self.logger.error("Failure to rename the temporary file "+str(newpath_tmp)+" to "+str(newpath))
                    return False
                else:
                    time.sleep(0.5)

        self.filepath = newpath
        self.getFileInfo()
        return True   


    def exists(self):
        return os.path.exists(self.filepath)

        #write self.outputData in json self.filepath
    def writeout(self,empty=False):
        filepath = self.filepath
        outputData = self.data
        self.logger.info(filepath)

        try:
            with open(filepath,"w") as fi:
                if empty==False:
                    json.dump(outputData,fi)
        except Exception,e:
            self.logger.exception(e)
            return False
        return True

    def esCopy(self):
        if not self.exists(): return
        if self.filetype in TO_ELASTICIZE:
            esDir = os.path.join(self.dir,ES_DIR_NAME)
            if os.path.isdir(esDir):
                newpath = os.path.join(esDir,self.basename)
                retries = 5
                while True:
                    try:
                        shutil.copy(self.filepath,newpath)
                        break
                    except (OSError,IOError),e:
                        self.logger.exception(e)
                        retries-=1
                        if retries == 0:
                            raise e
                        else:
                            time.sleep(0.5)


    def merge(self,infile):
        defs,oldData = self.definitions,self.data["data"][:]           #TODO: check infile definitions 
        jsdfile = infile.jsdfile
        host = infile.host
        newData = infile.data["data"][:]

        self.logger.debug("old: %r with new: %r" %(oldData,newData))
        result=Aggregator(defs,oldData,newData).output()
        self.logger.debug("result: %r" %result)
        self.data["data"] = result
        self.data["definition"] = jsdfile
        self.data["source"] = host

        if self.filetype==STREAMDQMHISTOUTPUT:
            self.inputs.append(infile)
        else:
            self.writeout()

    def updateData(self,infile):
        self.data["data"]=infile.data["data"][:]

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

