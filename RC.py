#!/usr/bin/env/python

# The goal is to provide information about certification for all DPGs for run taken in the last 2 days
# 
# python RC.py --min=246908 --max=300000 --group=Collisions15 --infile=Collisions15.txt
#

from optparse import OptionParser
from xml.dom.minidom import parseString
from rrapi import RRApi, RRApiError
from datetime import date, timedelta
import xmlrpclib
import sys, os, os.path, time, re

parser = OptionParser()
parser.add_option("-m", "--min", dest="min", type="int", default=246908, help="Minimum run")
parser.add_option("-M", "--max", dest="max", type="int", default=300000, help="Maximum run")
parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False, help="Print more info")
parser.add_option("-n", "--notes", dest="notes", type="string", default="notes.txt", help="Text file with notes")
parser.add_option("-g", "--group", dest="group", type="string", default="Cosmics16", help="Text file with run list")
parser.add_option("-a", "--allrun", dest="allrun", action="store_true", default=True, help="Show all runs in the table")
(options, args) = parser.parse_args()

options.verbose = False

groupName = options.group

runsel = ""
runsel = '>= %d and <= %d'%(options.min,options.max)

html = ""
runlist = {}

# List of DET/POG and RR workspace name correspondence
DetWS = {'PIX': 'Tracker', 'STRIP': 'Tracker', 'ECAL': 'Ecal', 'ES': 'Ecal', \
       'HCAL': 'Hcal', 'CSC': 'Csc', 'DT': 'Dt', 'RPC': 'Rpc', 'TRACK': 'Tracker', \
         'MUON': 'Muon', 'JETMET': 'Jetmet', 'EGAMMA': 'Egamma', \
         'HLT': 'Hlt', 'L1tmu': 'L1t', 'L1tcalo': 'L1t', 'LUMI': 'Lumi'}

URL = 'http://runregistry.web.cern.ch/runregistry/'
api = RRApi(URL, debug = True)

def getRR(whichRR, dataName, dsState, detPOG):
    global groupName, runreg, runlist, options, runsel
#    if options.verbose: print "Date of yesterday", str(date.today() - timedelta(days=1))
    firstDay = str(date.today() - timedelta(days=5))
    sys.stderr.write("Querying %s RunRegistry for %s runs...\n" % (whichRR,dataName));
    if whichRR == "GLOBAL":        
        mycolumns = ['runNumber','datasetState']
    else:
        mycolumns = ['%s' % detPOG.lower() ,'ranges','runNumber','datasetState']
    if options.verbose: print mycolumns
    text = ''
    fname = "RR_%s.%s.xml" % (detPOG,groupName)
    if options.verbose: print "Writing RR information in file %s" % fname
##Query RR
    if api.app == "user":
        text = api.data(workspace = whichRR.upper(), table = 'datasets', template = 'xml', columns = mycolumns, filter = {'runNumber': '%s' % runsel, 'runClassName':"like '%%%s%%'"%groupName,'datasetName': "like '%%%s%%'"%dataName}, tag='LATEST')        

    ##write xml output to file
        log = open(fname,"w"); 
        log.write(text); log.close()
    ##Get and Loop over xml data
    dom = ''; domP = None
    domB = '';
    try:
        dom  = parseString(text)
    except:
        ##In case of a non-Standard RR output (dom not set)
        print "Could not parse RR output"

    splitRows = 'RunDatasetRow'+whichRR 
    if options.verbose: print "splitRows", splitRows
# Getting info on the Bfield and the run taken in the last 2 days
    if whichRR == "GLOBAL" and dataName == "Online": 
        text_bfield = api.data(workspace = 'GLOBAL', table = 'runsummary', template = 'xml', columns = ['number','bfield','events'], filter = {"runClassName": "like '%%%s%%'"%groupName, "startTime": ">= %s" % firstDay,"datasets": {"rowClass": "org.cern.cms.dqm.runregistry.user.model.RunDatasetRowGlobal", "datasetName": "like %Online%"}}, tag= 'LATEST')
        log = open("RR_bfield.xml","w");
        log.write(text_bfield); log.close()
        try:
            domB  = parseString(text_bfield)
        except:
        ##In case of a non-Standard RR output (dom not set)
            print "Could not parse RR output"

    if whichRR == 'GLOBAL': splitRows = 'RunDatasetRowGlobal'
    ##Protection against null return
    if domB: dataB = domB.getElementsByTagName('RunSummaryRowGlobal')
    else: dataB =[]
    for i in range(len(dataB)):
        ##Get run#
        bfield = -1
        run = int(dataB[i].getElementsByTagName('number')[0].firstChild.data)
        bfield = dataB[i].getElementsByTagName('bfield')[0].firstChild.data
        events = int(dataB[i].getElementsByTagName('events')[0].firstChild.data)
        if run not in runlist: runlist[run] = {'B':bfield}
        if whichRR == 'GLOBAL' and dataName == 'Online':
            runlist[run]['RR_bfield'] = float(bfield)
            runlist[run]['RR_events'] = int(events)
    runs = runlist.keys(); runs.sort(); runs.reverse()
    print runs[0]

def getCertification(det, run, runlistdet):
    global groupName, runreg, runlist, options, runsel
    mycolumns = ['%s' % det.lower() ,'ranges','runNumber','datasetState']
    fname = "RR_%s.%s.xml" % (det,groupName)
    if options.verbose: print "Reading RR information in file %s" % fname
    whichRR = DetWS.get(det)
    ##Protection against null return
    splitRows = 'RunDatasetRow'+whichRR 
    ##Get and Loop over xml data
    log = open(fname)
    text = "\n".join([x for x in log])
    dom = '';
    try:
        dom  = parseString(text)
    except:
        ##In case of a non-Standard RR output (dom not set)
        print "Could not parse RR output"
    if dom: data = dom.getElementsByTagName(splitRows)
    else: data =[]
    comm = ""
    for i in range(len(data)):
        ##Get run#
        if int(data[i].getElementsByTagName('runNumber')[0].firstChild.data) == run:
            if options.verbose: print "---- Run ---- ", run        
            mydata = data[i]
            state = mydata.getElementsByTagName('datasetState')[0].firstChild.data
            print "STATE", state
            isopen = (state  == "OPEN")
            lumis= 0
            status = mydata.getElementsByTagName(mycolumns[0])[0].getElementsByTagName('status')[0].firstChild.data
            if options.verbose: print status
            comm = (mydata.getElementsByTagName(mycolumns[0])[0].getElementsByTagName('comment')[0].toxml()).replace('<comment>','').replace('</comment>','').replace('<comment/>','')
            verdict = status 
            if options.verbose: print "  -",run,verdict
        ##Compile comments
            comment = ""
            if comm: comment += " "+comm
            print "----- RunList", run, isopen, verdict, comment
            runlistdet[0] = isopen 
            runlistdet[1] = verdict
            runlistdet[2] = comment 

def v2c(isopen,verdict):
    if isopen: return 'TODO'
    for X,Y in [('BAD','BAD'), ('bad','bad'), ('GOOD','GOOD'), ('TODO','TODO'), ('WAIT','WAIT'), ('Wait','Wait'),('SKIP','SKIP'),('N/A','SKIP'),('STANDBY','STANDBY'),('EXCLUDED','EXCL')]:
        if X in verdict: return Y

def p2t(pair):
    (isopen, verdict, comment) = pair
    if comment:
        return "%s <span title=\"%s\">[...]</span>" % (verdict, comment)
    else:
        return verdict

#    html += "</table></body></html>"
#    html += "</BR>"


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
<table>
""" % (groupName, time.ctime(), groupName, time.ctime())

getRR("GLOBAL", "Online", "= OPEN OR = SIGNOFF OR = COMPLETED", 'Global') 
#listDETPOG = ['CSC','DT','ECAL','ES','HCAL','PIX','RPC','STRIP','TRACK']
listDETPOG = ['L1tmu','CSC','DT','RPC','ECAL','ES','HCAL','PIX','STRIP','TRACK']
for i in range(len(listDETPOG)):
    print "--------- Checking ", listDETPOG[i]
    localws = DetWS.get(listDETPOG[i])
    getRR("%s" % localws, "Express" , "= OPEN OR = SIGNOFF OR = COMPLETED", listDETPOG[i]) 

print "Done"

#html += "<tr><th>Run</th><th>B-field</th><th>CSC</th><th>DT</th><th>ECAL</th><th>ES</th><th>HCAL</th><th>PIX</th><th>RPC</th><th>STRIP</th><th>TRACKING</th></tr>"
html += "<tr><th>Run</th><th>B-field</th><th>Events</th><th>L1T muon</th><th>CSC</th><th>DT</th><th>RPC</th><th>ECAL</th><th>ES</th><th>HCAL</th><th>PIX</th><th>STRIP</th><th>TRACKING</th></tr>"
runs = runlist.keys(); runs.sort(); runs.reverse()
print "ALL RUNS: " , runs , "\n"

for r in runs:
    R = runlist[r]
    html += "<tr><th>%d</th><td class='num'>%.1f T</td><td class='num'>%d</td></td>" % (r, runlist[r]['RR_bfield'], runlist[r]['RR_events'])
    All_comments=''
    for i in range(len(listDETPOG)):
        localws = DetWS.get(listDETPOG[i])
        cert = ([False,'WAIT',''])
        getCertification("%s" % listDETPOG[i], r, cert)
        print "---- Certification for ", listDETPOG[i] , cert
        if options.verbose: 
            print "localws", localws
        html += "<td class='%s'>%s</td>" % (v2c(cert[0],cert[1]), p2t(cert))
        print v2c(cert[0],cert[1]), p2t(cert)

html += "</table></body></html>"
certday = date.today().strftime("%Y%m%d")
out = open("/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Cosmics16/CertSummary/status.%s.html" % groupName, "w")
out.write(html.encode('utf-8'))
out.close()

