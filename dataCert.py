#! /usr/bin/env python
################################################################################
# 
#
# $Author: smaruyam $
# $Date: 2012/12/14 17:02:45 $
# $Revision: 1.14 $
#
#
# Marco Rovere = marco.rovere@cern.ch
#
################################################################################

import re, json, sys, ConfigParser, os, string, commands, time, socket
from rrapi import RRApi, RRApiError

class Certifier():
    
    cfg='runreg.cfg'
    OnlineRX = "%Online%ALL"
    EXCL_LS_BITS = ('jetmet','muon','egamma')
    
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
        self.qflist  = CONFIG.get('Common','QFLAGS').split(',')

        self.bfield_thr  = '-0.1'
        self.bfield_min  = '-0.1'
        self.bfield_max  = '4.1'

        self.dcslist = CONFIG.get('Common','DCS').split(',')
        self.jsonfile = CONFIG.get('Common','JSONFILE')
        self.beamene     = []
        self.dbs_pds_all = ""
        self.online_cfg  = "FALSE"
        self.usedbs = False
        self.dsstate = ""
        self.useDBScache = "False"
        self.cacheFiles = []
        self.predefinedPD = ["/MinimumBias/Run2012A-v1/RAW","/MinimumBias/Run2012B-v1/RAW","/MinimumBias/Run2012C-v1/RAW","/MinimumBias/Run2012D-v1/RAW"]
        self.component = []
        self.nolowpu = "FALSE"

        print "First run ", self.runmin
        print "Last run ", self.runmax
        print "Dataset name ", self.dataset
        print "Group name ", self.group
        print "Quality flags ", self.qflist
        print "DCS flags ", self.dcslist

        for item in cfglist:
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
            if "ONLINE" in item[0].upper():
                self.online_cfg = item[1]
            if "DSSTATE" in item[0].upper():
                self.dsstate = item[1]
            if "DBSCACHE" in item[0].upper():
                self.useDBScache = item[1]
            if "CACHEFILE" in item[0].upper():
                self.cacheFiles = item[1].split(',')
            if "COMPONENT" in item[0].upper():
                self.component = item[1].split(',')
                print 'COMPONENT ', self.component
            if "NOLOWPU" in item[0].upper():
                self.nolowpu = item[1]
                print 'NoLowPU', self.nolowpu
                        
        self.dbs_pds = self.dbs_pds_all.split(",")

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
                self.filter.setdefault("dataset", {})\
                                                  .setdefault("filter", {})\
                                                  .setdefault(sys.lower(), {})\
                                                  .setdefault("status", " = %s" % value)
        if self.nolowpu == "True":
            print "Removing low pile-up runs"
            self.filter.setdefault("lowLumiStatus", "isNull OR = false")

        for dcs in self.dcslist:
            if dcs != "NONE":
                self.filter.setdefault(dcs.lower()+"Ready", "isNull OR  = true")
                if self.verbose: print dcs
            
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
        if self.usedbs:
            dbsjson=get_dbsjson(self, self.dbs_pds_all, self.runmin, self.runmax)
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
        if self.verbose:
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

def get_dbsjson(self, datasets, runmin, runmax):
    unsorted={}
    for ds in  datasets.split(","):
        if self.useDBScache == "True" and (ds in self.predefinedPD) :
            for cacheName in self.cacheFiles:
                cacheFile = open(cacheName)
                for line in cacheFile:
                    runlumi=line.split()
                    if len(runlumi) > 1:
                        if runlumi[0].isdigit():
                            run=runlumi[0]
                            if run not in unsorted.keys():
                                unsorted[run]=[]
                            for lumi in runlumi[1:]:
                                unsorted[run].append(int(lumi))
                cacheFile.close()
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

if __name__ == '__main__':
    cert = Certifier(sys.argv, verbose=False)
    cert.generateFilter()
    cert.generateJson()
    cert.writeJson()
