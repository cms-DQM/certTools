#!/usr/bin/env/python

# The goal is to provide information about certification for all DPGs for run taken in the last 2 days
#
# -- old   python RC.py --min=270000 --max=999999 --group=Collisions18 --infile=Collisions18.txt
# -- new

import sys
import time
import json
import logging
import argparse
from datetime import date, timedelta

from rhapi import RhApi

def get_bfield_events(workspace, datasetName, runClass, days, write_to_file):
    """
    get list of runs from specified start time containing bfield, events for them
    some doc string here
    workspace: RunRegistry workspace name
    datasetName: Dataset name which we look for #TO-DO: do we need it?
    runClass: type of run Collisions18, Cosmics etc.
    """
    logging.info("getting bfield and events for runs")

    runlist = {}
    firstDay = str(date.today() - timedelta(days=days))
    __query = ("select r.runnumber, r.bfield, r.events "
            "from runreg_global.runs_on r where r.run_class_name like '%%%s%%' "
            "and r.starttime >= to_date('%s','yyyy-MM-dd')") % (
                    runClass, firstDay)

    logging.debug("query to RR: %s" % (__query))

    try:
        rr_data = api.json(__query)
    except Exception as ex:
        logging.error("Error while using RestHub API: %s" % (ex))
        sys.exit(-1)

    #save bField file for now
    if write_to_file:
        logging.debug("Writing RR information in file RR_bfield_new.json")
        with open("RR_bfield_new.json","w") as out_f:
            out_f.write(json.dumps(rr_data, indent=4))

    for el in rr_data["data"]:
        ##Get run#
        bfield = -1

        run = int(el[0])
        bfield = el[1]
        events = el[2]

        if run not in runlist:
            runlist[run] = {}
            runlist[run]['RR_bfield'] = float(bfield)
            # some runs has None in integer field. example: 315105
            runlist[run]['RR_events'] = int(events) if events else 0

    runs = runlist.keys()
    runs.sort(reverse=True)

    logging.debug("list of runs: %s" % (runs))
    logging.debug("runlist: %s" % (json.dumps(runlist)))
    return runlist

def getRR(min_run, datasetName, workspace, datasetClass, columns,
        file_name, write_to_file):
    """
    method to query RR with specified input:
    minimum run we start from
    dataset_name: Online, Express etc
    workspace: workspace name
    datasetClass: dataset class name Collisions18
    """
    logging.info("getRRParams: %s %s %s %s" % (min_run, datasetName,
            workspace, datasetClass))

    firstDay = str(date.today() - timedelta(days=10))
    logging.debug("firstDay: %s" % (firstDay))

    text = ''
    fname = "RR_%s.%s_new.json" % (file_name, datasetClass)
    logging.debug("mycolumns: %s" % (columns))

    ##TO-DO: hope the table alias is 'r' all the time...
    __sql_columns = []

    for el in columns:
        __sql_columns.append("r.%s" % (el))

    logging.debug("%s %s %s %s %s" % (
            ",".join(__sql_columns), workspace.lower(),
            min_run, datasetClass, datasetName))

    __query = ("select %s from runreg_%s.datasets_off r "
        "where r.run_number >= %s and r.run_class_name like '%%%s%%' "
        "and r.rda_name like '%%%s%%'") % (
                ",".join(__sql_columns), workspace.lower(),
                min_run, datasetClass, datasetName)

    logging.debug("RR query: %s" % (__query))

    try:
        rr_data = api.json(__query, inline_clobs=True)
    except Exception as ex:
        logging.error("Error while using RestHub API: %s" % (ex))
        sys.exit(-1)

    rr_data = to_useful_format(rr_data)

    if write_to_file:
        logging.debug("Writing RR information in file %s" % (fname))
        ## write json output to file
        with open(fname,"w") as out_file:
            out_file.write(json.dumps(rr_data, indent=4))

    return rr_data

def to_useful_format(in_data):
    """
    converts list of reuslt columns to {key:[result]} where key is run
    """
    new_format = {}
    for el in in_data["data"]:
        new_format[el[1]] = el

    return new_format

def the_ctpps_special(in_data):
    """
    parse ctpps data for 6 columns and aggregate to one info field:
    ("RP45_210" == 'GOOD' or "RP45_220" == 'GOOD' or "RP45_CYL" == 'GOOD')
    and
    ("RP56_210" == 'GOOD' or "RP56_220" == 'GOOD' or "RP56_CYL" == 'GOOD')

    also concat all comments to single field.
    return data of single POG: CTPPS
    """
    ## TO-DO: hope ctpps doesn't add new columns or change the column order
    __ctpps_data = {}
    #iterate over all runs
    for el in in_data:
        all_ctpps_comments = ""
        statuses = []
        #move the workspace_name, run_num, dataset_state
        __ctpps_data[el] = [in_data[el][0], in_data[el][1], in_data[el][2]]

        for i in xrange(3, 15):
            if i%2 == 0:
                #its a comment
                if in_data[el][i]:
                    all_ctpps_comments += "%s\n" % (in_data[el][i])
            else:
                #its a status
                statuses.append(in_data[el][i])

        if ('GOOD' in statuses[:3]) and ('GOOD' in statuses[3:]):
            __ctpps_data[el].append('GOOD')
        else:
            logging.debug("ctpps column set to BAD")
            __ctpps_data[el].append('BAD')

        __ctpps_data[el].append(all_ctpps_comments)

    return __ctpps_data

def v2c(isopen,verdict):
    if isopen:
        return 'TODO'

    for X,Y in [('BAD','BAD'), ('bad','bad'), ('GOOD','GOOD'), ('TODO','TODO'),
            ('WAIT','WAIT'), ('Wait','Wait'), ('SKIP','SKIP'), ('N/A','SKIP'),
            ('STANDBY','STANDBY'), ('EXCLUDED','EXCL')]:

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
    parser = argparse.ArgumentParser(
            description='Make weekly certification HTML for run cordination',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-m", "--min",
            dest="min", type=int, default=314472,
            help="Minimum run")
    parser.add_argument("-M", "--max",
            dest="max", type=int, default=999999,
            help="Maximum run")
    parser.add_argument("-d", "--days",
            dest="days", type=int, default=10,
            help="Number of last days look for runs")
    parser.add_argument("-f", "--files",
            dest="rr_files",action="store_true", default=False,
            help="Store RunRegistry's api data to files")
    parser.add_argument("-v", "--verbose",
            dest="verbose", action="store_true", default=False,
            help="Print more info")
    parser.add_argument("-g", "--group",
            dest="group", type=str, default="Collisions18",
            help="Run type: Collisions/Cosmics etc.")


    options = parser.parse_args()

    if options.verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S', level=log_level)

    logging.debug("verbose:%s" % (options.verbose))

    runlist = {}
    run_pog_data = {}

    ## Definition of global map of workspaces and their representative columns in RR DB
    map_DB_to_column = {"CSC": {"workspace": "CSC",
                                "DB_column": "RDA_CMP_CSC"},
                        "CTPPS": {"workspace": "CTPPS", # ctpps has 6 columns
                                "DB_column": ["RDA_CMP_RP45_210", "RDA_CMP_RP45_220",
                                        "RDA_CMP_RP45_CYL", "RDA_CMP_RP56_210",
                                        "RDA_CMP_RP56_220", "RDA_CMP_RP56_CYL"]
                                },
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

    new_url = "http://vocms00170:2113"
    dev_url = "http://vocms0185/rhapi"
    api = RhApi(new_url, debug=False)

    sorted_list_of_POGS = ['CSC', 'CTPPS', 'DT', 'ECAL', 'ES', 'HCAL', 'HLT', 'L1tmu',
            'L1tcalo', 'RPC', 'PIX', 'STRIP', 'TRACK']

    #we get list of runs, their bfield and number of events
    runlist = get_bfield_events("GLOBAL", "Online", options.group,options.days,
            options.rr_files)

    for pog in sorted_list_of_POGS:
        logging.info("Cheking %s worspace for Express runs" % (pog))
        columns = ['rda_wor_name' , 'run_number', 'rda_state']
        if pog != "CTPPS":
            db_column = map_DB_to_column[pog.upper()]['DB_column'].lower()
            columns.append(db_column)
            columns.append(db_column + "_comment")
            run_pog_data[pog] = getRR(options.min, "Express",
                    map_DB_to_column[pog.upper()]["workspace"],
                    options.group, columns, pog, options.rr_files)

        else:
            for col in map_DB_to_column[pog.upper()]['DB_column']:
                columns.append(col.lower())
                columns.append(col.lower() + "_comment")

            ctpps_data = getRR(options.min, "Express",
                    map_DB_to_column[pog.upper()]["workspace"],
                    options.group, columns, pog, options.rr_files)

            run_pog_data[pog] = the_ctpps_special(ctpps_data)

    logging.debug("%s" % (run_pog_data))

    logging.info("Finished checking for express runs")

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

    html += "<tr><th>Run</th><th>B-field</th><th>Events</th>"
    for el in sorted_list_of_POGS:
        html += "<th>%s</th>" % (el)
    html+= "</tr>"

    runs = runlist.keys()
    runs.sort(reverse=True)

    logging.info("ALL RUNS: %s" % (runs))

    for ind, r in enumerate(runs):
        #R = runlist[r]
        html += "<tr><th>%d</th><td class='num'>%.1f T</td><td class='num'>%d</td></td>" % (
            r, runlist[r]['RR_bfield'], runlist[r]['RR_events'])

        for pog in sorted_list_of_POGS:
            logging.debug("current POG: %s run_index: %s" % (pog, ind))
            if r not in run_pog_data[pog]:
                logging.error("POG doesn't have data. Please check run:%s pog:%s" % (
                        r, pog))

                cert = ([False,'WAIT',''])
            else:
                __isopen = run_pog_data[pog][r][2] == "OPEN"
                __column_status = run_pog_data[pog][r][3]
                __comment = run_pog_data[pog][r][4]
                cert = ([__isopen, __column_status, __comment])

            html += "<td class='%s'>%s</td>" % (v2c(cert[0], cert[1]), p2t(cert))

    html += "</table></body></html>"
    certday = date.today().strftime("%Y%m%d")
    out = open("status_%s_new.html" % (options.group), "w")
    out.write(html.encode('utf-8'))
    out.close()
