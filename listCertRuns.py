from optparse import OptionParser
from rrapi import RRApi, RRApiError
import sys

##Run classification
groupName = "Collisions12"
#groupName = "Cosmics12"
prompt = ['like Prompt']
#prompt = ['Collisions']

parser = OptionParser()
parser.add_option("-m", "--min", dest="min", type="int", default=190456, help="Minimum run")
parser.add_option("-M", "--max", dest="max", type="int", default=999999, help="Maximum run")
parser.add_option("-d", "--day", dest="day", type="string", default="mon", help="Day for run list creation: use mon or wed")
(options, args) = parser.parse_args()

verbose = False

runonline=''
runsignoff=''
runcomplete=''

runlist=[]

URL = 'http://runregistry.web.cern.ch/runregistry/'
api = RRApi(URL, debug = True)

def getRR(whichRR, dataName):
    global groupName, runreg, runlist, options, runonline, runsignoff, runcomplete
    sys.stderr.write("Querying %s RunRegistry for %s runs...\n" % (whichRR,dataName));
    mycolumns = ['runNumber']
    text = ''
    ##Query RR
    if api.app == "user": 
        if dataName == "Online":
            runonline = api.data(workspace = whichRR, table = 'datasets', template = 'csv', columns = mycolumns, filter = {'runNumber':'>= %d and <= %d'%(options.min,options.max),'runClassName':"like '%%%s%%'"%groupName,'datasetName':"like '%%%s%%'"%dataName})
            if verbose: print "Online query ", runonline
            if not "runNumber" in runonline:
                if verbose: print "Failed RR query for Online runs"
                return
        if dataName == "Prompt":
#        if dataName == "Collisions2012":
            if options.day == "wed":
                runsignoff = api.data(workspace = whichRR, table = 'datasets', template = 'csv', columns = mycolumns, filter = {'runNumber':'>= %d and <= %d'%(options.min,options.max),'runClassName':"like '%%%s%%'"%groupName,'datasetName':"like '%%%s%%'"%dataName,'datasetState': '= SIGNOFF'})
                if verbose: print "Offline ", runsignoff
                if not "runNumber" in runsignoff:
                    print "Failed RR query for Offline runs"
                    return
            if options.day == "mon" or options.day == "all" :
                runcomplete = api.data(workspace = whichRR, table = 'datasets', template = 'csv', columns = mycolumns, filter = {'runNumber':'>= %d and <= %d'%(options.min,options.max),'runClassName':"like '%%%s%%'"%groupName,'datasetName':"like '%%%s%%'"%dataName,'datasetState': '= COMPLETED'})
                if verbose: print "Offline ", runcomplete
                if not "runNumber" in runcomplete:
                    print "Failed RR query for Offline runs"
                    return

##Start running RR queries
getRR("GLOBAL", "Online")
getRR("GLOBAL", "Prompt")
                
for line in runonline.split("\n"):
    if line.isdigit():
        if options.day == "mon":
            addrun = True            
            for roff in runcomplete.split("\n"):
                if roff == line:
                    if verbose: print "Run is already in completed state"
                    addrun = False
            if addrun: runlist.append(line)

        if options.day == "wed":
            for roff in runsignoff.split("\n"):
                if roff == line:
                    runlist.append(line)                    
                
        if options.day == "all":
            for roff in runcomplete.split("\n"):
                if roff == line:
                    runlist.append(line)                
        
print "Run list for ", options.day
runlist.sort()

list=''

for item in runlist:
    list +=item+ " "

print list
