from optparse import OptionParser
from rrapi import RRApi, RRApiError
import sys, os, os.path, time, re
from math import copysign

groupName = "Collisions12"
prompt = ['like Prompt']

parser = OptionParser()
parser.add_option("-m", "--min", dest="min", type="int", default=194270, help="Minimum run")
parser.add_option("-M", "--max", dest="max", type="int", default=200000, help="Maximum run")
(options, args) = parser.parse_args()

verbose = False

runonline=''
#runsignoff=''
#runcomplete=''

runlist={}

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

##Start running RR queries
getRR("GLOBAL", "Online")
#getRR("GLOBAL", "Prompt")
                
#runonline.sort()

parkingData = {}
output = open("parkingdata.txt", "w");
output.write("fill\trun\tlumi_pb\n");

def compare(a, b):
    diff = 0
    diff = a.fill - b.fill
    if diff:
        return diff
    l = a.lumi - b.lumi
    if l:
        return int(copysign(1, l))
    diff = a.run - b.run
    if diff:
        return diff
    return diff

class Element:
    fill = 0
    run = 0
    lumi = 0.0

    def dump(self):
        return "Fill: %d Run: %d Lumi: %f" % (self.fill, self.run, self.lumi)

lista = []

for run in runonline.split("\n"):
    if run.isdigit():
        try:
            os.system("lumiCalc2.py -r %s -b stable overview -o lumi.tmp" % run)
            out = [ l for l in open("lumi.tmp","r")]
#            print out[1]
            (myrun,myls,deliv,lsrange,mylumi) = out[1].split(",", 5)
            fill = int(myrun.split(":")[1])
            run = int(myrun.split(":")[0])
#            print "Fill and run ", fill, run
            lslumi = ( int(myls), float(mylumi)/1.0e6 )
            output.write("%d\t%d\t%.3f\n" % (fill, run, lslumi[1]) )
            el = Element()
            el.fill = int(fill)
            el.run = int(run)
            el.lumi = float(lslumi[1])
            lista.append(el)
        except IOError:
            pass
        except ValueError:
            lslumi = (-1,0)

fill = 0
runlist = ""
for e in sorted(lista, cmp=compare, reverse=True):
    if e.fill != fill:
        print e.dump()
        runlist = str(e.run) + " " + runlist
        fill = e.fill

print runlist



