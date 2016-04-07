#! /usr/bin/env python
################################################################################
# 
#
# $Author: smaruyam $
# $Date: 2012/12/14 17:02:45 $
# $Revision: 1.15 $ 18.06.2015 $
#
#
# Marco Rovere = marco.rovere@cern.ch
# Laura Borello = Laura.Borrello@cern.ch 
# Ringaile Placakyte = ringaile@mail.desy.de
#
################################################################################

import re, json, sys, ConfigParser, os, string, commands, time, socket
from rrapi import RRApi, RRApiError

class Certifier():
    
    cfg='runreg.cfg'
    OnlineRX = "%Online%ALL"
    EXCL_LS_BITS = ('jetmet','muon','egamma')
    EXCL_RUN_BITS = ('all')
    
    def __init__(self,argv,verbose=False):
        self.verbose = verbose
        if len(argv)==2:
            self.cfg = argv[1]
        else:
            self.cfg = Certifier.cfg
        self.qry = {}
        self.qry.setdefault("GOOD", "isNull OR = true")
        self.qry.setdefault("BAD", " = false")
        self.readConfig()
        
    def readConfig(self):
        CONFIG = ConfigParser.ConfigParser()
        if self.verbose:
            print 'Reading configuration file from %s' % self.cfg
        CONFIG.read(self.cfg)
        cfglist = CONFIG.items('Common')
        self.dataset = CONFIG.get('Common','DATASET')
        self.group   = CONFIG.get('Common','GROUP')
        self.address = CONFIG.get('Common','RUNREG')
        self.runmin  = CONFIG.get('Common','RUNMIN')
        self.runmax  = CONFIG.get('Common','RUNMAX')
        self.runlist = "" 
        for item in cfglist:
            if "RUNLIST" in item[0].upper():
                self.runlist = item[1].split(" ")
        self.qflist  = CONFIG.get('Common','QFLAGS').split(',')

        self.bfield_thr  = '-0.1'
        self.bfield_min  = '-0.1'
        self.bfield_max  = '4.1'

        self.injection  = "%"
        self.dcslist = CONFIG.get('Common','DCS').split(',')
        self.jsonfile = CONFIG.get('Common','JSONFILE')
        self.beamene     = []
        self.dbs_pds_all = ""
        self.online_cfg  = "FALSE"
        self.usedbs = False
        self.useDAS = False
        self.dsstate = ""
        self.useDBScache = "False"
        self.useBeamPresent = "False"
        self.useBeamStable = "False"
        self.cacheFiles = []
        self.predefinedPD = ["/Commissioning/Run2015A-v1/RAW","/ZeroBias/Run2015B-v1/RAW"]
        self.component = []
        self.nolowpu = "True"

        print "First run ", self.runmin
        print "Last run ", self.runmax
        if len(self.runlist)>0:
            print "List of runs ", self.runlist, " (",len(self.runlist), " runs)"
        print "Dataset name ", self.dataset
        print "Group name ", self.group
        print "Quality flags ", self.qflist
        print "DCS flags ", self.dcslist

        for item in cfglist:
            if "INJECTION" in item[0].upper():
                self.injection  = item[1]
            if "BFIELD_THR" in item[0].upper():
                self.bfield_thr = item[1]
            if "BFIELD_MIN" in item[0].upper():
                self.bfield_min = item[1]
            if "BFIELD_MAX" in item[0].upper():
                self.bfield_max = item[1]
            if "BEAM_ENE" in item[0].upper():
                self.beamene = item[1].split(',')
            if "DBS_PDS" in item[0].upper():
                self.dbs_pds_all = item[1]
                self.usedbs = True
            if "USE_DAS" in item[0].upper():
                self.useDAS = item[1]
            if "ONLINE" in item[0].upper():
                self.online_cfg = item[1]
            if "DSSTATE" in item[0].upper():
                self.dsstate = item[1]
            if "DBSCACHE" in item[0].upper():
                self.useDBScache = item[1]
            if "BEAMPRESENT" in item[0].upper():
                self.useBeamPresent = item[1]
                print 'Use Beam Present Flag', self.useBeamPresent
            if "BEAMSTABLE" in item[0].upper():
                self.useBeamStable = item[1]
                print 'Use Beam Stable Flag', self.useBeamStable
            if "CACHEFILE" in item[0].upper():
                self.cacheFiles = item[1].split(',')
            if "COMPONENT" in item[0].upper():
                self.component = item[1].split(',')
                print 'COMPONENT ', self.component
            if "NOLOWPU" in item[0].upper():
                self.nolowpu = item[1]
                print 'NoLowPU', self.nolowpu

        self.dbs_pds = self.dbs_pds_all.split(",")

        print "Injection schema ", self.injection

        if self.useDAS == "True":
            self.usedbs = False
        print "Using DAS database: ", self.useDAS
        print "Using Cache? : ", self.useDBScache

        self.online = False
        if "TRUE" == self.online_cfg.upper() or \
               "1" == self.online_cfg.upper() or \
               "YES" == self.online_cfg.upper():
            self.online = True

        try:
            self.bfield_min = float(self.bfield_min)
        except:
            print "Minimum BFIELD value not understood: ", self.bfield_min
            sys.exit(1)
        try:
            self.bfield_max = float(self.bfield_max)
        except:
            print "Maximum BFIELD value not understood: ", self.bfield_max
            sys.exit(1)
        try:
            self.bfield_thr = float(self.bfield_thr)
        except:
            print "Threshold BFIELD value not understood: ", self.bfield_thr
            sys.exit(1)
        if self.bfield_thr > self.bfield_min:
            self.bfield_min = self.bfield_thr

        for e in range(0, len(self.beamene)):
            try:
                self.beamene[e] = float(self.beamene[e])
                if self.verbose:
                    print "Beam Energy ", self.beamene
            except:
                print "BEAMENE value not understood: ", self.beamene
                sys.exit(1)                

    def generateFilter(self):
        self.filter = {}

        self.filter.setdefault("dataset", {})\
                                          .setdefault("rowClass", "org.cern.cms.dqm.runregistry.user.model.RunDatasetRowGlobal")

        for qf in self.qflist:
            (sys,value) = qf.split(':')
            if self.verbose: print qf
            if sys != "NONE":
                # Check if the bit is not excluded to avoide filter on LS for Egamma, Muon, JetMET
                if len([i for i in self.EXCL_LS_BITS if i == sys.lower()]) == 0:
                    self.filter.setdefault(sys.lower()+"Status", self.qry[value])
                # Check run flag
                if (self.EXCL_RUN_BITS != sys.lower()):
                    self.filter.setdefault("dataset", {})\
                                                  .setdefault("filter", {})\
                                                  .setdefault(sys.lower(), {})\
                                                  .setdefault("status", " = %s" % value)


        if self.nolowpu == "True":
            print "Removing low pile-up runs"
            self.filter.setdefault("lowLumiStatus", "isNull OR = false")
        else:
            print "Selecting ONLY low pile-up runs"
            self.filter.setdefault("lowLumiStatus", "true")

        for dcs in self.dcslist:
            if dcs != "NONE":
                self.filter.setdefault(dcs.lower()+"Ready", "isNull OR  = true")
#                self.filter.setdefault(dcs.lower(), "isNull OR  = true")
                if self.verbose: print dcs
            
        if self.useBeamPresent == "True":
            print "Removing LS with no beam present"
            self.filter.setdefault("beam1Present", "isNull OR  = true")
            self.filter.setdefault("beam2Present", "isNull OR  = true")

        if self.useBeamStable == "True":
            print "Removing LS with non-stable beams"
            self.filter.setdefault("beam1Stable", "isNull OR  = true")
            self.filter.setdefault("beam2Stable", "isNull OR  = true")

        if self.online:
            self.filter.setdefault("dataset", {})\
                                              .setdefault("filter", {})\
                                              .setdefault("datasetName", "like %s" % Certifier.OnlineRX)
            self.filter.setdefault("dataset", {})\
                                              .setdefault("filter", {})\
                                              .setdefault("online", " = true")
        else:
            datasetQuery = ''
            for i in self.dataset.split(): 
                datasetQuery += ' like "%s" OR' % i.split(":")[0]
            self.filter.setdefault("dataset", {})\
                                              .setdefault("filter", {})\
                                              .setdefault("datasetName", " like %s" % datasetQuery)

        self.filter.setdefault("runNumber", ">= %d AND <= %d " %(int(self.runmin), int(self.runmax)))
        self.filter.setdefault("dataset", {})\
                                          .setdefault("filter", {})\
                                          .setdefault("runClassName", self.group)
        self.filter.setdefault("dataset", {})\
                                          .setdefault("filter", {})\
                                          .setdefault("run", {})\
                                          .setdefault("rowClass", "org.cern.cms.dqm.runregistry.user.model.RunSummaryRowGlobal")
        self.filter.setdefault("dataset", {})\
                                          .setdefault("filter", {})\
                                          .setdefault("run", {})\
                                          .setdefault("filter",{})\
                                          .setdefault("bfield", "> %.1f AND <  %.1f " % (self.bfield_min, self.bfield_max) )

        self.filter.setdefault("dataset", {})\
                                          .setdefault("filter", {})\
                                          .setdefault("run", {})\
                                          .setdefault("filter", {})\
                                          .setdefault("injectionScheme", " like %s " % self.injection )

        self.filter.setdefault("cmsActive", "isNull OR = true")

        for comp in self.component:
             if comp != 'NONE':
                 self.filter.setdefault("dataset", {})\
                                                   .setdefault("filter", {})\
                                                   .setdefault("run", {})\
                                                   .setdefault("filter",{})\
                                                   .setdefault(comp.lower()+"Present", " = true")

        if len(self.dsstate):
            self.filter.setdefault("dataset", {})\
                                          .setdefault("filter", {})\
                                          .setdefault("datasetState", " = %s"  % self.dsstate)
        if len(self.beamene):

            eneQuery = '{lhcEnergy} IS NULL OR {lhcEnergy} = 0 '
            for e in self.beamene:
                energyLow = e - 400 
                if energyLow < 0:
                    energyLow = 0
                energyHigh = e + 400 
                eneQuery += 'OR ( {lhcEnergy} >= %.1d AND {lhcEnergy} <= %.1d) ' % (energyLow, energyHigh)
            self.filter.setdefault("dataset", {})\
                                              .setdefault("filter", {})\
                                              .setdefault("run", {})\
                                              .setdefault("query", eneQuery)
        
        if self.verbose:
            print json.dumps(self.filter)

    def generateJson(self):
        try:
            self.api = RRApi(self.address, debug = self.verbose)
        except RRApiError, e:
            print e
            sys.exit(1)
        self.cert_json = self.api.data(workspace = 'GLOBAL'\
                                       , table = 'datasetlumis'\
                                       , template = 'json'\
                                       , columns = ['runNumber', 'sectionFrom', 'sectionTo']\
                                       , tag = 'LATEST'\
                                       , filter = self.filter)
        if self.verbose:
            print "Printing JSON file ", json.dumps(self.cert_json)
        self.convertToOldJson()

        dbsjson={}
        if self.useDBScache == "True":
            dbsjson=get_cachejson(self, self.dbs_pds_all) 
        elif self.usedbs:
            dbsjson=get_dbsjson(self, self.dbs_pds_all, self.runmin, self.runmax, self.runlist)
        elif self.useDAS:   
            dbsjson=get_dasjson(self, self.dbs_pds_all, self.runmin, self.runmax, self.runlist)
        else:
# special case, e.g. cosmics which do not need DB or cache file
            print "\nINFO: no cache or DB option was selected in cfg file" 
           
        if self.useDBScache == "True" or \
           self.usedbs or \
           self.useDAS:

            if len(dbsjson)==0: 
                print "\nERROR, dbsjson contains no runs, please check!" 
                sys.exit(1)
            if self.verbose:
                print "Printing dbsjson ", dbsjson
            for element in self.cert_old_json:
                combined=[]
                dbsbad_int=invert_intervals(self.cert_old_json[element])
                if self.verbose:
                    print " debug: Good Lumi ", self.cert_old_json[element] 
                    print " debug:  Bad Lumi ", dbsbad_int 
                for interval in  dbsbad_int:
                    combined.append(interval)
            
                if element in dbsjson.keys():
                    if self.verbose:
                        print " debug: Found in DBS, Run ", element, ", Lumi ", dbsjson[element]
                    dbsbad_int=invert_intervals(dbsjson[element])
                    if self.verbose:
                        print " debug DBS: Bad Lumi ", dbsbad_int 
                else:
                    dbsbad_int=[[1,9999]]
                for interval in  dbsbad_int:
                    combined.append(interval)
                combined=merge_intervals(combined)
                combined=invert_intervals(combined) 
                if len(combined)!=0:
                    self.cert_old_json[element]=combined 

        if self.verbose:
            print json.dumps(self.cert_old_json)

    def convertToOldJson(self):
        old_json = {}
        self.cert_old_json = {}
        for block in self.cert_json:
            if len(block) == 3:
             runNum = block['runNumber']
             lumiStart = block['sectionFrom']
             lumiEnd = block['sectionTo']
             if self.verbose:
                 print " debug: Run ", runNum, " Lumi ", lumiStart, ", ", lumiEnd

# impose the selection of runs from the run list if given in cfg file 
# (in a list of run accessed from RR) same later applied to list accessed from DAS
             if len(self.runlist)>0:
	        foundr = False
	        for runinl in self.runlist:
	           if runinl.startswith('"'):
		      runinl = runinl[1:]
		   if runinl.endswith('"'):
	   	      runinl = runinl[:-1]
	  	   if int(runNum) == int(runinl):
		      foundr = True
#		      print "selecting run fom the list = ", runNum, runinl

  	        if foundr:
                   old_json.setdefault(str(runNum), []).append([lumiStart, lumiEnd])
                   if self.verbose:
                      print old_json[str(runNum)]
	     else: 
                old_json.setdefault(str(runNum), []).append([lumiStart, lumiEnd])
                if self.verbose:
                   print old_json[str(runNum)]

        for block in old_json:
            temp = []
            temp = merge_intervals2(old_json[block])
            self.cert_old_json.setdefault(block, temp)
            if self.verbose:
                print "Merging Done on Run ", block,
                print " Interval ", temp 

    def writeJson(self):
        js = open(self.jsonfile, 'w+')
        json.dump(self.cert_old_json, js, sort_keys=True)
        js.close()
#       print json file name
        print " "
        print "-------------------------------------------"
        print "Json file: %s written.\n" % self.jsonfile
        

def invert_intervals(intervals,min_val=1,max_val=9999):
    if not intervals:
        return []
    intervals=merge_intervals(intervals)
    intervals = sorted(intervals, key = lambda x: x[0])
    result = []
    if min_val==-1:
        (a,b)=intervals[0]
        min_val=a
    if max_val==-1:
        (a,b)=intervals[len(intervals)-1]
        max_val=b

    curr_min=min_val
    for (x,y) in intervals:
        if x>curr_min:
            result.append((curr_min,x-1))
        curr_min=y+1
    if curr_min<max_val:
        result.append((curr_min,max_val))

    return result

def merge_intervals(intervals):
    if not intervals:
        return []
    intervals = sorted(intervals, key = lambda x: x[0])
    result = []
    (a, b) = intervals[0]
    for (x, y) in intervals[1:]:
        if x <= b:  
            b = max(b, y)
        else:
            result.append((a, b))
            (a, b) = (x, y)
    result.append((a, b))
    return result

def merge_intervals2(intervals):
    if not intervals:
        return []
    intervals = sorted(intervals, key = lambda x: x[0])
    result = []
    (a, b) = intervals[0]
    for (x, y) in intervals[1:]:
        if x <= b+1:    
            b = max(b, y)
        else:
            result.append((a, b))
            (a, b) = (x, y)
    result.append((a, b))
    return result


def get_cachejson(self, datasets):
    unsorted={}
    lumirangesjson=[]
    lumirangejson={}
    fileformatjson = False
    for ds in  datasets.split(","):
        if (ds in self.predefinedPD) :
            for cacheName in self.cacheFiles:
                cacheFile = open(cacheName)
                for line in cacheFile:
# 
                    if '[' in line:
#                        print "List in cache file is json format "
                        fileformatjson = True

                    runlumi=line.split()
                    if len(runlumi) > 1:
                        if runlumi[0].isdigit():
                            run=runlumi[0]

                            if fileformatjson:
                                runlumic=runlumi[1:]
                                lumirange=[]
                                for i, v in enumerate(runlumic):
                                    if not i%2:
                                        lowlumi=int(v.replace('[','').replace(']','').replace(',','')) 
                                    else:
                                        highlumi=int(v.replace('[','').replace(']','').replace(',','')) 
                                        lumi=range(lowlumi,highlumi+1)
                                        lumirange+=lumi
                                if run not in unsorted.keys():
                                    unsorted[run]=[]
                                    unsorted[run]=lumirange

			    else:
                                if run not in unsorted.keys():
                                    unsorted[run]=[]
                                    for lumi in runlumi[1:]:
                                        unsorted[run].append(int(lumi))
                cacheFile.close()
  
    sorted={}
    for run in unsorted.keys():
       lumilist=unsorted[run]
       lumilist.sort()
       sorted[run]=lumilist

    dbsjson={}
    for run in sorted.keys():
       lumilist=sorted[run]
       lumiranges=[]
       lumirange=[]
       lumirange.append(lumilist[0])
       lastlumi=lumilist[0]
       for lumi in lumilist[1:]:
           if lumi>lastlumi+1:
               lumirange.append(lastlumi)
               lumiranges.append(lumirange)
               lumirange=[]
               lumirange.append(lumi)
           lastlumi=lumi
       if len(lumirange)==1:
           lumirange.append(lastlumi)
           lumiranges.append(lumirange)
       dbsjson[run]=lumiranges

    return dbsjson

def get_dbsjson(self, datasets, runmin, runmax, runlist):
    unsorted={}
    for ds in  datasets.split(","):

        command='' 
        if len(runlist)>0:
           command='dbs search --query="find run,lumi where dataset=%s and run in (%s)"' % (ds, runlist)
           print "\nWARNING: dbs seach will only work if RunList contains less then 650 runs (this option to become obsolete!)" 
        else: 
           command='dbs search --query="find run,lumi where dataset=%s and run >=%s  and run<=%s"' % (ds, runmin, runmax)
        print command

        (status, out) = commands.getstatusoutput(command)
        if status: 
            sys.stderr.write(out)
            print "\nERROR on dbs command: %s\nHave you done cmsenv?" % command
            sys.exit(1)
        for line in out.split('\n'):
            fields=line.split()
            if len(fields)!=2:
                continue
            if fields[0].isdigit() and fields[1].isdigit():
                run=fields[0]
                lumi=int(fields[1])
                if run not in unsorted.keys():
                     unsorted[run]=[]
                unsorted[run].append(lumi)

    sorted={}
    for run in unsorted.keys():
        lumilist=unsorted[run]
        lumilist.sort()
        sorted[run]=lumilist

    dbsjson={}
    for run in sorted.keys():
        lumilist=sorted[run]
        lumiranges=[]
        lumirange=[]
        lumirange.append(lumilist[0])
        lastlumi=lumilist[0]
        for lumi in lumilist[1:]:
            if lumi>lastlumi+1:
                lumirange.append(lastlumi)
                lumiranges.append(lumirange)
                lumirange=[]
                lumirange.append(lumi)
            lastlumi=lumi
        if len(lumirange)==1:
            lumirange.append(lastlumi)
            lumiranges.append(lumirange)
        dbsjson[run]=lumiranges

    return dbsjson


def get_dasjson(self, datasets, runmin, runmax, runlist):
    unsorted={}
    for ds in  datasets.split(","):
        for runnm in range(int(runmin), int(runmax)+1):
            foundrun = False
            if len(runlist)>0:
#                 if str(runnm) in runlist: 
#                 print ">>> run in list  = ", runnm
                for runl in runlist:
                    if runl.startswith('"'):
                        runl = runl[1:]
                    if runl.endswith('"'):
                        runl = runl[:-1]
                    if str(runnm) in runl: 
                        foundrun = True
            else:
                foundrun = True

            if foundrun:
                command='das_client.py --query="lumi,run dataset=%s run=%s system=dbs3" --format=json --das-headers --limit=0' % (ds, runnm)

                print command
                (status, out) = commands.getstatusoutput(command)

                if status: 
                    sys.stderr.write(out)
                    print "\nERROR on das command: %s\nHave you done cmsenv?" % command
                    sys.exit(1)

                js=json.loads(out)
#               print "JSON FORMAT", js
                try:
                    js['data'][0]['run'][0]['run_number']
                except:
                    continue

                run = js['data'][0]['run'][0]['run_number']
                lumi = js['data'][0]['lumi'][0]['number']

                if run not in unsorted.keys():
                   unsorted[run]=[]
                   for l in lumi:
	               unsorted[run].append(l)

            dasjson={}
            for run in unsorted.keys():
	        dasjson[str(run)]=unsorted[run]

    return dasjson

if __name__ == '__main__':
    cert = Certifier(sys.argv, verbose=False)
    cert.generateFilter()
    cert.generateJson()
    cert.writeJson()

# test
