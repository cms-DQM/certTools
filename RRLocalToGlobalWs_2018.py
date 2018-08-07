#!/usr/bin/env/python

# The goal of the script is to provide information about differences between certification in Global and Local workspaces
# 
# HOW TO RUN THE SCRIPT
#
#python compLocalGlbWs_2015.py --min=271000 --max=999999 --refprocess=Collisions2017 --process=Collisions2017 --group=Collisions17 --infile=Collisions17.txt
#
# ADDING THE OPTION infile=filename.txt LIST OF RUN IS TAKEN FROM THE FILE AND NOT FROM THE FULL SET OF RUNS IN ONLINE RR

from optparse import OptionParser
from xml.dom.minidom import parseString
from rrapi import RRApi, RRApiError
from datetime import date
import xmlrpclib
import sys, os, os.path, time, re

html = ""

parser = OptionParser()
parser.add_option("-m", "--min", dest="min", type="int", default=271000, help="Minimum run")
parser.add_option("-M", "--max", dest="max", type="int", default=999999, help="Maximum run")
parser.add_option("--min-ls",    dest="minls",  type="int", default="10", help="Ignore runs with less than X lumis (default 10)")
parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False, help="Print more info")
parser.add_option("-n", "--notes", dest="notes", type="string", default="notes.txt", help="Text file with notes")
parser.add_option("-p", "--process", dest="process", type="string", default="PromptReco", help="Dataset name for last certification")
parser.add_option("-r", "--refprocess", dest="refprocess", type="string", default="PromptReco", help="Dataset name to be used as reference")
parser.add_option("-g", "--group", dest="group", type="string", default="Collisions18", help="Text file with run list")
parser.add_option("-i", "--infile", dest="infile", type="string", default="Collisions18.txt", help="Text file with run list")
parser.add_option("-a", "--allrun", dest="allrun", action="store_true", default=True, help="Show all runs in the table")
(options, args) = parser.parse_args()

options.verbose = False

groupName = options.group

runsel = ""
runsel = '>= %d and <= %d'%(options.min,options.max)

if options.infile:
    runsel = ""
    filename=options.infile
    inputfile = open(filename,"r")
    print "Opening file ", filename, "which contains the run list"
    selruns = [file.strip() for file in open(filename)]
    for run in selruns:
        if options.verbose: print run 
        runsel += " OR = " + run

print "Run selection ", runsel

notes = {}
if options.notes:
    try:
        nfile = open(options.notes, "r");
        for l in nfile:
            m = re.match(r"\s*(\d+)\s*:?\s+(.*)", l)
            if m:
                notes[int(m.group(1))] = m.group(2)
    except IOError:
        print "Couldn't read notes file", options.notes

lumiCache = {};
lumiCacheName = "lumi-by-run-grin.txt" 
if options.verbose:
    print "lumiCacheName ",lumiCacheName
try:
    lumiFile = open(lumiCacheName, "r")
    print "Opening file ", lumiCacheName
    for l in lumiFile:
        m = re.match(r"(\d+)\s+(\d+)\s+([0-9.]+).*", l)
        if m:
            lumiCache[int(m.group(1))] = int(m.group(2)), float(m.group(3))
except IOError:
   pass 

runlist = {}

# List of DET/POG and RR workspace name correspondence
DetWS = {'PIX': 'Tracker', 'STRIP': 'Tracker', 'ECAL': 'Ecal', 'ES': 'Ecal', \
       'HCAL': 'Hcal', 'CSC': 'Csc', 'DT': 'Dt', 'RPC': 'Rpc', 'TRACK': 'Tracker', \
         'MUON': 'Muon', 'JETMET': 'Jetmet', 'EGAMMA': 'Egamma', \
         'HLT': 'Hlt', 'L1tmu': 'L1t', 'L1tcalo': 'L1t', 'LUMI': 'Lumi'}
# withCastor included DetWS = {'PIX': 'Tracker', 'STRIP': 'Tracker', 'ECAL': 'Ecal', 'ES': 'Ecal', \
#       'HCAL': 'Hcal', 'CSC': 'Csc', 'DT': 'Dt', 'RPC': 'Rpc', 'TRACK': 'Tracker', \
#         'MUON': 'Muon', 'JETMET': 'Jetmet', 'EGAMMA': 'Egamma', \
#         'HLT': 'Hlt', 'L1tmu': 'L1t', 'L1tcalo': 'L1t', 'LUMI': 'Lumi', 'CASTOR': 'Castor'}


URL = 'http://runregistry.web.cern.ch/runregistry/'
api = RRApi(URL, debug = True)

def getRR(whichRR, dataName, dsState, detPOG):
    global groupName, runreg, runlist, options, runsel
    sys.stderr.write("Querying %s RunRegistry for %s runs...\n" % (whichRR,dataName));
    mycolumns = ['%s' % detPOG.lower() ,'ranges','runNumber','datasetState']
    if "OR" in dataName:
        newdataName = ''
        for i in dataName.split("OR"):
            if options.verbose: print i
            newdataName += " like '%%%s%%' OR" % i
        if options.verbose:
            print newdataName
    text = ''
    fname = "RR_%s.%s.%s.xml" % (whichRR.upper(),groupName,dataName)
    if options.verbose: print "Writing RR information in file %s" % fname
##Query RR
    if api.app == "user":
        if "OR" in dataName:
            text = api.data(workspace = whichRR.upper(), table = 'datasets', template = 'xml', columns = mycolumns, filter = {'runNumber': '%s' % runsel,'runClassName':"like '%%%s%%'"%groupName,'datasetName': "%s" %newdataName, 'datasetState': "%s" % dsState}, tag='LATEST')
        else:
            text = api.data(workspace = whichRR.upper(), table = 'datasets', template = 'xml', columns = mycolumns, filter = {'runNumber': '%s' % runsel,'runClassName':"like '%%%s%%'"%groupName,'datasetName': "like '%%%s%%'"%dataName, 'datasetState': "%s" % dsState}, tag='LATEST')        
    ##write xml output to file
    log = open(fname,"w"); 
    log.write(text); log.close()
    ##Get and Loop over xml data
    dom = ''; domP = None
    try:
        dom  = parseString(text)
    except:
        ##In case of a non-Standard RR output (dom not set)
        print "Could not parse RR output"
    splitRows = 'RunDatasetRow'+whichRR 
    if options.verbose: print "splitRows", splitRows
    ##Protection against null return
    if dom: data = dom.getElementsByTagName(splitRows)
    else: data =[]
    if domP: dataP = domP.getElementsByTagName(splitRows)
    else: dataP =[]
    state = "WAIT"
    for i in range(len(data)):
        ##Get run#
        run = int(data[i].getElementsByTagName('runNumber')[0].firstChild.data)
        if options.verbose: print "---- Run ---- ", run        
        mydata = data[i]
        for X in dataP:
            if int(X.getElementsByTagName('runNumber')[0].firstChild.data) == run:
                print "Run ",run, ": found manual patch for ",whichRR,groupName,dataName,
                mydata = X; break
        state = mydata.getElementsByTagName('datasetState')[0].firstChild.data
        isopen = (state  == "OPEN")
        lumis= 0
        if run not in runlist:
            runlist[run] = {'ls':lumis}
        goodp = mydata.getElementsByTagName(mycolumns[0])[0].getElementsByTagName('status')[0].firstChild.data == 'GOOD'
        status = mydata.getElementsByTagName(mycolumns[0])[0].getElementsByTagName('status')[0].firstChild.data
        if options.verbose: print status
        commp = (mydata.getElementsByTagName(mycolumns[0])[0].getElementsByTagName('comment')[0].toxml()).replace('<comment>','').replace('</comment>','').replace('<comment/>','')
        if goodp:
            verdict = "Good" 
        else:
            verdict = status 
        if options.verbose: print "  -",run,lumis,verdict
        ##Compile comments
        comment = ""
        if commp: comment += " "+commp
        runlist[run]['RR_'+whichRR+"_"+dataName] = [ isopen, verdict, comment, state]
        if options.verbose:
            print "RunList", run, isopen, verdict, comment, state

def v2c(isopen,verdict):
    for X,Y in [('Bad','BAD'), ('bad','bad'), ('Good','GOOD'), ('TODO','TODO'), ('WAIT','WAIT'), ('WAIT','NA'),('SKIP','SKIP'),('N/A','SKIP'),('EXCLUDED','EXCL'),('STANDBY','STANDBY'),('BAD','BAD')]:
        if X in verdict: return Y


def p2t(pair):
    (isopen, verdict, comment, state) = pair
#    print isopen, verdict, comment, state
#    (isopen, verdict, comment) = pair
    if comment:
        return "%s <span title=\"%s\">[...]</span>" % (verdict, comment)
    else:
        return verdict

def infolumi():
    if options.verbose: print "Creating information for luminosity"
    runs = runlist.keys(); runs.sort();
    if options.verbose: print "Getting luminosities"
    newcache = open("lumi-by-run-grin.txt", "w");
    if newcache:
        print "file with lumi information is available"
    else:
        print "No file with lumi information"            
        newcache = open("lumi-by-run-grin.txt", "w");
        newcache.write("run\tls\tlumi_pb\n");
    for run in runs:
        if options.verbose: print "Run to be analyzed ", run
        if run not in lumiCache:               
            print " - ",run
            lslumi = (-1,0)
            try:
                if options.verbose: print "Running lumiCalc2 for run ", run 
                out = [ l for l in open("lumi.tmp","r")]
                if (len(out) <= 1): raise ValueError
                cols = out[1].strip().split(",");
                myrun = cols[0]
                myls = cols[1]
                delivered = cols[2]
                mylumi = cols[-1]                    
                myrun = myrun.split(":")[0]
                if int(myrun) == run:
                    lslumi = ( int(myls), float(mylumi)/1.0e6 )
                    if options.verbose: print "\t- %6d, %4d, %6.3f" % (run, lslumi[0], lslumi[1])
#                    print "\t- %6d, %4d, %6.3f" % (run, lslumi[0], lslumi[1])
            except IOError:
                pass
            except ValueError:
                lslumi = (-1,0)
            lumiCache[run] = lslumi
        else:
            print "data in lumi file"
        if lumiCache[run][0] != -1:
            print "writing lumi info in file ", newcache
            newcache.write("%d\t%d\t%.3f\n" % (run, lumiCache[run][0], lumiCache[run][1]))
    newcache.close()
    
def createhtmlinfo(detPOG, localws):
    global html

    wsName=str(localws).upper()
    runs = runlist.keys(); runs.sort(); 
    if options.verbose: print "runs in createhtml", runs
#    print ""
#    print "%-6s |  %-15s | %-15s | %-15s | %-15s | %s " % ("RUN","GLOBAL FLAG","%s FLAG" % localws , "Run state - GLB", "Run state - %s" %localws, "NOTES")
#    print "%-6s |  %-15s | %-15s | %-15s | %-15s | %s " % ("-"*6, "-"*15, "-"*15, "-"*15, "-"*15, "-"*30)
    text = ''
    html += "<table><tr><th>Run</th><th>GLOBAL</th><th>%s</th><th>Run state - GLB</th><th>Run state - %s</th><th>NOTES</th></tr>" % ( detPOG, detPOG)
    for r in runs:
        R = runlist[r]
        if options.verbose:
            print "--- Printing R", R, "for run", r
        All_comments=''
        (oldcert, newcert) = ([False,'WAIT','NA'], [False,'WAIT','','NA'])
        if 'RR_%s_%s' % ('Global', options.refprocess) in R:
            oldcert = R['RR_%s_%s' %('Global', options.refprocess)] 
            All_comments+= oldcert[1]
            if options.verbose:
                print "Global", oldcert
        if options.verbose: print "localws", localws, R
        if 'RR_%s_%s' % (localws, options.process) in R:
            newcert = R['RR_%s_%s' %( localws, options.process)]
            All_comments+=newcert[1]
            if options.verbose:
                print "Local", newcert
        note = notes[r] if r in notes else " "
        if oldcert[1] != newcert[1]:
            if options.verbose: print "---- run with different flag?", r
            text += str(r) + ' ' 
            if options.verbose:
                print "Checking %s" % detPOG
                print "Comment is different for ", oldcert[1], newcert[1]
#            print "%6d |  %-15s | %-15s | %s " % (r, oldcert[1], newcert[1], note)            
            html += "<tr><th>%d</th>" % r

            for X in (oldcert, newcert):
                print oldcert
                print newcert
                print "--- Info 1", v2c(X[0],X[1])
                ##print "--- Info run", r, p2t(X)
                html += "<td class='%s'>%s</td>" % (v2c(X[0],X[1]), p2t(X))
            html += "<td>%s</td><td>%s</td>" % (oldcert[3], newcert[3])
            html += "<td>%s</td></tr>\n" % note;
        else:
            if options.verbose: print "Run with same flag"
            if options.allrun:
                html += "<tr><th>%d</th>" % r
                for X in (oldcert, newcert):
                    html += "<td class='%s'>%s</td>" % (v2c(X[0],X[1]), p2t(X))
                html += "<td>%s</td><td>%s</td>" % (oldcert[3], newcert[3])
                html += "<td>%s</td></tr>\n" % note;
    html += "</table></body></html>"
    html += "</BR>"

# Writing list of runs with different flags in a text file
    if text:
        html += "Run with different flags %s </BR><hr>" % text
        if options.verbose: print "Difference ---------"
    else:
        if options.verbose: print "-------- No difference in flag"
        html += "No difference found in certification for %s <hr>" % detPOG
        text += 'No difference found!!!'
         
    diffrun = open('DiffRunsFor%s.txt' %detPOG,'w')
    if options.verbose: print "Creating file with different flags", diffrun
    diffrun.write(text)
    diffrun.close()       

html = """
<html>
<head>
<title>Certification Status, %s (%s)</title>
  <style type='text/css'>
    body { font-family: "Candara", sans-serif; }
    td.EXCL { background-color: orange; }
    td.BAD  { background-color: rgb(255,100,100); }    
    td.STANDBY { background-color: yellow ; }
    td.WARNING { background-color: yellow ; } 
    td.GOOD { background-color: rgb(100,255,100); }
    td.WAIT { background-color: rgb(200,200,255); }
    td.SKIP { background-color: rgb(200,200,200); }
    td, th { padding: 1px 5px; 
             background-color: rgb(200,200,200); 
             margin: 0 0;  }
    td.num { text-align: right; padding: 1px 10px; }
    table, tr { background-color: black; }
  </style>
</head>
<body>
<h1>Certification Status, %s (%s)</h1>
<hr>
""" % (groupName, time.ctime(), groupName, time.ctime())

#print options.process
#print options.refprocess

listDETPOG = ['CSC','DT','ECAL','ES','HCAL','HLT','L1tmu','L1tcalo','LUMI','PIX','RPC','STRIP','EGAMMA','JETMET','MUON','TRACK']
for i in range(len(listDETPOG)):
#    if options.verbose: 
    print "--------- Checking ", listDETPOG[i]

    html += """<h3>Certification Status for %s </h3><table>""" % listDETPOG[i]

    localws = DetWS.get(listDETPOG[i])
    print "local workspace", localws

    getRR("Global", "%s" % options.refprocess, "= OPEN OR = SIGNOFF OR = COMPLETED", listDETPOG[i]) 
    getRR("%s" % localws, "%s" % options.process , "= OPEN OR = SIGNOFF OR = COMPLETED", listDETPOG[i]) 

# a protection against RR failure should be added

#    if i == 0: infolumi()
    createhtmlinfo(listDETPOG[i], localws)
                   
print "Done"

html += "</table></body></html>"
certday = date.today().strftime("%Y%m%d")
#out = open("/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions17/13TeV/RRComparison/status.%s.%s.html" % (groupName, certday), "w")
out = open("status.%s.html" % groupName, "w")
out.write(html.encode('utf-8'))
out.close()

