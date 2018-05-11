#!/usr/bin/env/python

# The goal is to provide information about certification for all DPGs for run taken in the last 2 days
#
# -- old   python RC.py --min=270000 --max=999999 --group=Collisions18 --infile=Collisions18.txt
# -- new

import xmlrpclib
import os, os.path, time, re
import argparse
import logging
import subprocess
import sys
import json
#from optparse import OptionParser

from xml.dom.minidom import parseString
#from rrapi import RRApi, RRApiError
from rhapi import RhApi
from datetime import date, timedelta


def get_bfield_events(workspace, datasetName, runClass):
    """
    get list of runs from specified start time containing bfield, events for them
    some doc string here
    workspace: RunRegistry workspace name
    datasetName: Dataset name which we look for #TO-DO: do we need it?
    runClass: type of run Collisions18, Cosmics etc.
    """
    logging.info("getting bfield and events for runs")

    runlist = {}
    firstDay = str(date.today() - timedelta(days=10))
    __query = ("select r.runnumber, r.bfield, r.events "
            "from runreg_global.runs r where r.run_class_name like '%%%s%%' "
            "and r.starttime >= to_date('%s','yyyy-MM-dd')") % (
                    runClass, firstDay)

    logging.debug("query to RR: %s" % (__query))
    rr_data2 = api.json(__query)

    #save bField file for now
    #TO-DO: bfield should be JSON
    with open("RR_bfield_new.json","w") as out_f:
        out_f.write(json.dumps(rr_data2, indent=4))



    for el in rr_data2["data"]:
        ##Get run#
        bfield = -1

        run = int(el[0])
        bfield = el[1]
        events = el[2]

        if run not in runlist:
            runlist[run] = {'B': bfield}
        if workspace == 'GLOBAL' and datasetName == 'Online':
            runlist[run]['RR_bfield'] = float(bfield)
            runlist[run]['RR_events'] = int(events)

    runs = runlist.keys()
    runs.sort()
    runs.reverse()

    logging.debug("list of runs: %s" % (runs))
    logging.debug("runlist: %s" % (json.dumps(runlist)))
    return runlist

def getRR(min_run, datasetName, workspace, datasetClass):
    """
    method to query RR with specified input:
    minimum run we start from
    dataset_name: Online, Express etc
    workspace: workspace name
    datasetClass: dataset class name Collisions18
    """
    logging.info("getRRParams: %s %s %s %s" % (min_run, datasetName, workspace, datasetClass))

    firstDay = str(date.today() - timedelta(days=10))
    logging.debug("firstDay: %s" % (firstDay))

    if workspace.upper() == "GLOBAL":
        mycolumns = ['run_number', 'rda_state']
    else:
        mycolumns = ['rda_wor_name' , 'run_number', 'rda_state']

    text = ''
    fname = "RR_%s.%s_new.json" % (workspace, datasetClass)
    logging.debug("mycolumns: %s" % (mycolumns))
    logging.debug("Writing RR information in file %s" % (fname))

    ##TO-DO: hope the table alias is 'r' all the time...
    __sql_columns = []

    for el in mycolumns:
        __sql_columns.append("r.%s" % (el))

    logging.debug("%s %s %s %s %s" % (
                ",".join(__sql_columns), workspace.lower(),
                min_run, datasetClass, datasetName))

    __query = ("select %s from runreg_%s.datasets r "
        "where r.run_number >= %s and r.run_class_name like '%%%s%%' "
        "and r.rda_name like '%%%s%%'") % (
                ",".join(__sql_columns), workspace.lower(),
                options.min, datasetClass, datasetName)

    logging.debug("RR query: %s" % (__query))

    rr_data = api.json(__query)
    ##write xml output to file
    with open(fname,"w") as out_file:
        out_file.write(json.dumps(rr_data, indent=4))

def getCertification(det, run, runlistdet):
    global groupName, runreg, runlist, options, runsel
    mycolumns = ['%s' % det.upper() ,'ranges','runNumber','datasetState']
    fname = "RR_%s.%s.xml" % (det, groupName)
    if options.verbose:
        print "Reading RR information in file %s" % fname
    whichRR = DetWS.get(det)
    ##Protection against null return
    #splitRows = 'RunDatasetRow' + whichRR
    splitRows = 'row'
    ##Get and Loop over xml data
    log = open(fname)
    text = "\n".join([x for x in log])
    dom = '';
    try:
        dom  = parseString(text)
    except:
        ##In case of a non-Standard RR output (dom not set)
        print "Could not parse RR output"
    if dom:
        data = dom.getElementsByTagName(splitRows)
    else:
        data = []
    comm = ""
    for i in range(len(data)):
        ##Get run#
        if int(data[i].getElementsByTagName('RUN_NUMBER')[0].firstChild.data) == run:
            if options.verbose:
                print "---- Run ---- ", run
            mydata = data[i]
            state = mydata.getElementsByTagName('RDA_STATE')[0].firstChild.data
            print "STATE", state
            isopen = (state  == "OPEN")
            lumis = 0
            ##TO-DO:get a global dictionary with workspace and the columns to be displayed
            status = mydata.getElementsByTagName(mycolumns[0])[0].getElementsByTagName('status')[0].firstChild.data
            if options.verbose:
                print status
            comm = (mydata.getElementsByTagName(mycolumns[0])[0].getElementsByTagName('RDA_COMMENT')[0].toxml()).replace('<comment>','').replace('</comment>','').replace('<comment/>','')
            verdict = status
            if options.verbose:
                print "  -", run, verdict
        ##Compile comments
            comment = ""
            if comm:
                comment += " " + comm
            print "----- RunList", run, isopen, verdict, comment
            runlistdet[0] = isopen
            runlistdet[1] = verdict
            runlistdet[2] = comment

def v2c(isopen,verdict):
    if isopen:
        return 'TODO'

    for X,Y in [('BAD','BAD'), ('bad','bad'), ('GOOD','GOOD'), ('TODO','TODO'), ('WAIT','WAIT'), ('Wait','Wait'),('SKIP','SKIP'),('N/A','SKIP'),('STANDBY','STANDBY'),('EXCLUDED','EXCL')]:
        if X in verdict:
            return Y

def p2t(pair):
    (isopen, verdict, comment) = pair
    if isopen:
        return 'To be checked'
    if comment:
        return "%s <span title=\"%s\">[...]</span>" % (verdict, comment)
    else:
        return verdict

if __name__ == '__main__':
    #parser = OptionParser()
    parser = argparse.ArgumentParser(description='Make weekly certification HTML for run cordination')
    parser.add_argument("-m", "--min",
            dest="min", type=int, default=314472, help="Minimum run")
    parser.add_argument("-M", "--max",
            dest="max", type=int, default=999999, help="Maximum run")
    parser.add_argument("-v", "--verbose",
            dest="verbose", action="store_true", default=False, help="Print more info")
    parser.add_argument("-n", "--notes",
            dest="notes", type=str, default="notes.txt", help="Text file with notes")
    parser.add_argument("-g", "--group",
            dest="group", type=str, default="Collisions18", help="Text file with run list")
    parser.add_argument("-a", "--allrun",
            dest="allrun", action="store_true", default=True, help="Show all runs in the table")

    options = parser.parse_args()

    if options.verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S', level=log_level)

    #TO-DO:wtf if default is true
    logging.debug("verbose:%s" % (options.verbose))
    options.verbose = False

    groupName = options.group

    #runsel = ""
    #runsel = '>= %d and <= %d' % (options.min,options.max)

    runlist = {}

    # List of DET/POG and RR workspace name correspondence
    # DetWS = {'PIX': 'Tracker', 'STRIP': 'Tracker', 'ECAL': 'Ecal', 'ES': 'Ecal', \
    #         'HCAL': 'Hcal', 'CSC': 'Csc', 'DT': 'Dt', 'RPC': 'Rpc', 'TRACK': 'Tracker', \
    #         'MUON': 'Muon', 'JETMET': 'Jetmet', 'EGAMMA': 'Egamma', \
    #         'HLT': 'Hlt', 'L1tmu': 'L1t', 'L1tcalo': 'L1t', 'LUMI': 'Lumi'}#, 'CTPPS': 'ctpps'}

    ## Definition of global map of workspaces and their representative columns in RR DB
    # TO-DO: add global workspace and its columns
    # TO-DO: add multiple columns for comment, cause etc.
    map_DB_to_column = {"CSC": {"workspace": "CSC",
                                "DB_column": "RDA_CMP_CSC"},
                        # "CTPPS": {"workspace": "CTPPS",
                        #         "DB_column": "RDA_CMP_RP45_210"},  #CTPPS has 6 columns
                        "DT": {"workspace": "DT",
                                "DB_column": "RDA_CMP_DT"},
                        "ECAL": {"workspace": "ECAL",
                                "DB_column": "RDA_CMP_ECAL"},
                        "ES": {"workspace": "ECAL",
                                "DB_column": "RDA_CMP_ES"},
                        "EGAMMA": {"workspace": "EGAMMA",
                                "DB_column": "RDA_CMP_EGAMMA"},
                        "HCAL": {"workspace": "HCAL",
                                "DB_column": "RDA_CMP_HCAL"}, #this column is not displayed in userRR
                        "HLT": {"workspace": "HLT",
                                "DB_column": "RDA_CMP_HLT"},
                        "JETMET": {"workspace": "JETMET",
                                "DB_column": "RDA_CMP_JETMET"},
                        "L1TMU": {"workspace": "L1T",
                                "DB_column": "RDA_CMP_MUON"},
                        "L1TCALO": {"workspace": "L1T",
                                "DB_column": "RDA_CMP_JET"},
                        "LUMI": {"workspace": "LUMI",
                                "DB_column": "RDA_CMP_LUMI"},
                        "MUO": {"workspace": "MUO",
                                "DB_column": "RDA_CMP_MUON"},
                        "RPC": {"workspace": "RPC",
                                "DB_column": "RDA_CMP_RPC"},
                        "PIX": {"workspace": "TRACKER",
                                "DB_column": "RDA_CMP_PIXEL"},
                        "STRIP": {"workspace": "TRACKER",
                                "DB_column": "RDA_CMP_STRIP"},
                        "TRACK": {"workspace": "TRACKER",
                                "DB_column": "RDA_CMP_TRACKING"},
    }

    html = """
<html>
<head>
<title>Certification of Collision runs recorded in the last 10 days </BR>
(Last update on %s)</title>
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
<h1>Certification of Collision runs recorded in the last 10 days </BR>
(Last update on %s)</title>
</h1>
<table>
""" % (time.ctime(), time.ctime())

    #URL = 'http://runregistry.web.cern.ch/runregistry/'
    #api = RRApi(URL, debug=True)
    new_url = "http://vocms00170:2113"
    new_new_url = "http://vocms0185.cern.ch:2113"
    api = RhApi(new_url, debug=False)

    #we get list of runs, their bfield and number of events
    runlist = get_bfield_events("GLOBAL", "Online", groupName)


    getRR(options.min, "Online", 'Global', groupName)
    sys.exit(-1)

    #current one is below
    listDETPOG = ['HLT','L1tcalo','L1tmu','CSC','DT','RPC','ECAL','ES','HCAL','PIX','STRIP','TRACK']#,'CTPPS']
    listDETPOG = ['CSC','DT','RPC','ECAL']#,'CTPPS']
    sorted_list_of_POGS = ['CSC','CTPPS','DT','ECAL','ES','EGAMMA','HCAL','HLT','JETMET','L1tmu','L1tcalo','LUMI','MUON','RPC','PIX','STRIP','TRACK']
    # for now ignore CTPPS
    # TO-DO: add ctpps once we conclude that new API works
    # TO-DO: RR api add non mandatory variable for needed columns! Status,cause,comment
    sorted_list_of_POGS2 = ['CSC','DT','ECAL','ES','EGAMMA','HCAL','HLT','JETMET','L1tmu','L1tcalo','LUMI','MUON','RPC','PIX','STRIP','TRACK']
    for pog in sorted_list_of_POGS2[0]:
        logging.info("Cheking %s worspace for Express runs" % (pog))
        getRR("%s" % pog, "Express", pog)
    logging.info("Finished checking for express runs")
    sys.exit(-1)

    #html += "<tr><th>Run</th><th>B-field</th><th>CSC</th><th>DT</th><th>ECAL</th><th>ES</th><th>HCAL</th><th>PIX</th><th>RPC</th><th>STRIP</th><th>TRACKING</th></tr>"
    html += "<tr><th>Run</th><th>B-field</th><th>Events</th><th>HLT</th><th>L1T calo</th><th>L1T muon</th><th>CSC</th><th>DT</th><th>RPC</th><th>ECAL</th><th>ES</th><th>HCAL</th><th>PIX</th><th>STRIP</th><th>TRACKING</th><th>CTPPS</th></tr>"
    runs = runlist.keys()
    runs.sort()
    runs.reverse()
    print "ALL RUNS: " , runs , "\n"

    for r in runs:
        R = runlist[r]
        html += "<tr><th>%d</th><td class='num'>%.1f T</td><td class='num'>%d</td></td>" % (r, runlist[r]['RR_bfield'], runlist[r]['RR_events'])
        All_comments = ''
        for i in range(len(listDETPOG)):
            localws = DetWS.get(listDETPOG[i])
            cert = ([False,'WAIT',''])
            getCertification("%s" % listDETPOG[i], r, cert)
            print "---- Certification for ", listDETPOG[i] , cert
            if options.verbose:
                print "localws", localws
            html += "<td class='%s'>%s</td>" % (v2c(cert[0], cert[1]), p2t(cert))
            print v2c(cert[0],cert[1]), p2t(cert)

    html += "</table></body></html>"
    certday = date.today().strftime("%Y%m%d")
    out = open("status.%s.html" % groupName, "w")
    out.write(html.encode('utf-8'))
    out.close()
