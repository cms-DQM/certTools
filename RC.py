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
            runlist[run] = {}
            runlist[run]['RR_bfield'] = float(bfield)
            runlist[run]['RR_events'] = int(events)

    runs = runlist.keys()
    runs.sort(reverse=True)

    logging.debug("list of runs: %s" % (runs))
    logging.debug("runlist: %s" % (json.dumps(runlist)))
    return runlist

def getRR(min_run, datasetName, workspace, datasetClass, columns, file_name):
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

    text = ''
    fname = "RR_%s.%s_new.json" % (file_name, datasetClass)
    logging.debug("mycolumns: %s" % (columns))
    logging.debug("Writing RR information in file %s" % (fname))

    ##TO-DO: hope the table alias is 'r' all the time...
    __sql_columns = []

    for el in columns:
        __sql_columns.append("r.%s" % (el))

    logging.debug("%s %s %s %s %s" % (
            ",".join(__sql_columns), workspace.lower(),
            min_run, datasetClass, datasetName))

    __query = ("select %s from runreg_%s.datasets r "
        "where r.run_number >= %s and r.run_class_name like '%%%s%%' "
        "and r.rda_name like '%%%s%%'") % (
                ",".join(__sql_columns), workspace.lower(),
                min_run, datasetClass, datasetName)

    logging.debug("RR query: %s" % (__query))

    rr_data = api.json(__query)
    rr_data = to_useful_format(rr_data)
    ##write json output to file
    with open(fname,"w") as out_file:
        out_file.write(json.dumps(rr_data, indent=4))

    return rr_data

def to_useful_format(in_data):
    """
    converts list of reuslt columns to key:value/result where key is run
    """
    new_format = {}
    for el in in_data["data"]:
        new_format[el[1]] = el

    return new_format

def get_comment(comment):
    """
    make subprocess curl to fetch CLOB comment data from RestHub
    comment: comment from previous RR api call -> can be None or link to clob object
    """

    if not comment:
        return comment
    if comment.startswith("http"):
        p = subprocess.Popen(["curl", "-s", comment], stdout=subprocess.PIPE)
        out = p.communicate()[0]
        return out

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
    run_pog_data = {}

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
                        "EGAMMA": {"workspace": "EGAMMA", ## TO-DO: is egamma even used?
                                "DB_column": "RDA_CMP_EGAMMA"},
                        "HCAL": {"workspace": "HCAL",
                                "DB_column": "RDA_CMP_HCAL"}, #this column is not displayed in userRR
                        "HLT": {"workspace": "HLT",
                                "DB_column": "RDA_CMP_HLT"},
                        "JETMET": {"workspace": "JETMET", ## TO-DO: is JETMET used?
                                "DB_column": "RDA_CMP_JETMET"},
                        "L1TMU": {"workspace": "L1T",
                                "DB_column": "RDA_CMP_MUON"},
                        "L1TCALO": {"workspace": "L1T",
                                "DB_column": "RDA_CMP_JET"},
                        "LUMI": {"workspace": "LUMI", ## TO-DO: is LUMI used?
                                "DB_column": "RDA_CMP_LUMI"},
                        "MUON": {"workspace": "MUON",
                                "DB_column": "RDA_CMP_MUON"}, ## TO-DO: is MUON used?
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
<title>Certification of Collision runs recorded in the last 10 days</title>
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
""" % (time.ctime())

    #URL = 'http://runregistry.web.cern.ch/runregistry/'
    #api = RRApi(URL, debug=True)
    new_url = "http://vocms00170:2113"
    new_new_url = "http://vocms0185.cern.ch:2113"
    api = RhApi(new_url, debug=False)

    #we get list of runs, their bfield and number of events
    runlist = get_bfield_events("GLOBAL", "Online", groupName)


    ## TO-DO: do we need this call for Global dataset Table? -> nowhere used
    getRR(options.min, "Online", 'Global', groupName,
            ['run_number', 'rda_state'], 'Global')

    #current one is below
    listDETPOG = ['HLT','L1tcalo','L1tmu','CSC','DT','RPC','ECAL','ES','HCAL','PIX','STRIP','TRACK']#,'CTPPS']
    listDETPOG = ['CSC','DT','RPC','ECAL']#,'CTPPS']
    sorted_list_of_POGS = ['CSC','CTPPS','DT','ECAL','ES','HCAL','HLT','L1tmu','L1tcalo','RPC','PIX','STRIP','TRACK']
    # for now ignore CTPPS
    # TO-DO: add ctpps once we conclude that new API works
    # TO-DO: RR api add non mandatory variable for needed columns! Status,cause,comment
    sorted_list_of_POGS2 = ['CSC','DT','ECAL','ES', 'HCAL','HLT','L1tmu','L1tcalo','RPC','PIX','STRIP','TRACK']
    for pog in sorted_list_of_POGS2:
        logging.info("Cheking %s worspace for Express runs" % (pog))
        #def getRR(min_run, datasetName, workspace, datasetClass):
        #getRR(options.min, "Online", 'Global', groupName)
        columns = ['rda_wor_name' , 'run_number', 'rda_state']
        db_column = map_DB_to_column[pog.upper()]['DB_column'].lower()
        columns.append(db_column)
        columns.append(db_column + "_comment")
        run_pog_data[pog] = getRR(options.min, "Express",
                map_DB_to_column[pog.upper()]["workspace"],
                groupName, columns, pog)

        #run_pog_data[pog].sort(reverse=True)
    logging.debug("%s" % (run_pog_data))

    logging.info("Finished checking for express runs")

    html += "<tr><th>Run</th><th>B-field</th><th>Events</th>"
    for el in sorted_list_of_POGS2:
        html += "<th>%s</th>" % (el)
    html+= "</tr>"

    runs = runlist.keys()
    runs.sort(reverse=True)

    logging.info("ALL RUNS: %s" % (runs))

    for ind, r in enumerate(runs):
        #R = runlist[r]
        html += "<tr><th>%d</th><td class='num'>%.1f T</td><td class='num'>%d</td></td>" % (r, runlist[r]['RR_bfield'], runlist[r]['RR_events'])
        All_comments = ''
        for pog in sorted_list_of_POGS2:
            logging.debug("current POG: %s run_index: %s" % (pog, ind))
            if r not in run_pog_data[pog]:
                logging.error("POG doesn't have data. Please check run:%s pog:%s" % (r, pog))
                cert = ([False,'WAIT',''])
            else:
                __isopen = run_pog_data[pog][r][2] == "OPEN"
                __column_status = run_pog_data[pog][r][3]
                __comment = get_comment(run_pog_data[pog][r][4])
                cert = ([__isopen, __column_status, __comment])

            html += "<td class='%s'>%s</td>" % (v2c(cert[0], cert[1]), p2t(cert))

    html += "</table></body></html>"
    certday = date.today().strftime("%Y%m%d")
    out = open("status_%s_new.html" % (groupName), "w")
    out.write(html.encode('utf-8'))
    out.close()
