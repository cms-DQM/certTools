#!/usr/bin/env/python

# The goal of the script is to provide information about differences between certification in Global and Local workspaces
#
# HOW TO RUN THE SCRIPT
#
#python compLocalGlbWs_2015.py --min=271000 --max=999999 --refprocess=Collisions2017 --process=Collisions2017 --group=Collisions17 --infile=Collisions17.txt
#
# ADDING THE OPTION infile=filename.txt LIST OF RUN IS TAKEN FROM THE FILE AND NOT FROM THE FULL SET OF RUNS IN ONLINE RR

import re
import sys
import time
import argparse

from datetime import date

from rhapi import RhApi

def query_runregistry(query):
    """
    query run registry's tables using resthub API
    """
    try:
        api = RhApi("http://vocms00170:2113", debug=options.verbose)
        rr_data = api.json_all(query, inline_clobs=True)
    except Exception as ex:
        print("Error while using RestHub API: %s" % (ex))
        sys.exit(-1)

    return rr_data

def v2c(isopen,verdict):
    for X, Y in [('Bad','BAD'), ('bad','bad'), ('Good','GOOD'), ('TODO','TODO'),
            ('WAIT','WAIT'), ('WAIT','NA'), ('SKIP','SKIP'), ('N/A','SKIP'),
            ('EXCLUDED','EXCL'), ('STANDBY','STANDBY'), ('BAD','BAD')]:

        if X in verdict:
            return Y

def p2t(pair):
    (isopen, verdict, comment, state) = pair
    if comment:
        return "%s <span title=\"%s\">[...]</span>" % (verdict, comment)
    else:
        return verdict

def infolumi():
    if options.verbose:
        print("Creating information for luminosity")

    runs = runlist.keys()
    runs.sort()
    if options.verbose:
        print("Getting luminosities")

    newcache = open("lumi-by-run-grin.txt", "w")

    if newcache:
        print("file with lumi information is available")
    else:
        print("No file with lumi information")
        newcache = open("lumi-by-run-grin.txt", "w")
        newcache.write("run\tls\tlumi_pb\n")

    for run in runs:
        if options.verbose:
            print("Run to be analyzed: %s" % (run))
        if run not in lumiCache:
            print(" - %s" % (run))
            lslumi = (-1,0)
            try:
                if options.verbose:
                    print("Running lumiCalc2 for run: %s" % (run))
                out = [l for l in open("lumi.tmp", "r")]
                if (len(out) <= 1):
                    raise ValueError
                cols = out[1].strip().split(",")
                myrun = cols[0]
                myls = cols[1]
                delivered = cols[2]
                mylumi = cols[-1]
                myrun = myrun.split(":")[0]
                if int(myrun) == run:
                    lslumi = ( int(myls), float(mylumi)/1.0e6 )
                    if options.verbose:
                        print("\t- %6d, %4d, %6.3f" % (run, lslumi[0], lslumi[1]))
            except IOError:
                pass
            except ValueError:
                lslumi = (-1, 0)
            lumiCache[run] = lslumi
        else:
            print("data in lumi file")
        if lumiCache[run][0] != -1:
            print("writing lumi info in file: %s" % (newcache))
            newcache.write("%d\t%d\t%.3f\n" % (run, lumiCache[run][0], lumiCache[run][1]))

    newcache.close()

def createhtmlinfo(data, detPOG):
    text = ""
    html = ""
    runs = runlist.keys()
    runs.sort()

    if options.verbose:
        print("runs in createhtml: %s" % (runs))

    html += ("<table><tr><th>Run</th><th>GLOBAL</th><th>%s</th><th>Run state - GLB</th>"
            "<th>Run state - %s</th><th>NOTES</th></tr>") % (detPOG, detPOG)

    for r in runs:
        R = runlist[r]

        if options.verbose:
            print("\tPrinting R: %s for run: %s" % (R, r))

        All_comments = ''
        (oldcert, newcert) = ([False,'WAIT','NA'], [False,'WAIT','','NA'])

        if 'RR_%s_GLOBAL_%s' % (detPOG, options.refprocess) in R:
            oldcert = R['RR_%s_GLOBAL_%s' % (detPOG, options.refprocess)]
            All_comments += oldcert[1]
            if options.verbose:
                print("Global %s value: %s" % (detPOG, oldcert))

        if 'RR_%s_LOCAL_%s' % (detPOG, options.process) in R:
            newcert = R['RR_%s_LOCAL_%s' % (detPOG, options.process)]
            All_comments += newcert[1]
            if options.verbose:
                print("Local %s value: %s" % (detPOG, newcert))

        note = notes[r] if r in notes else " "
        if oldcert[1] != newcert[1]:
            if options.verbose:
                print("\trun with different flag: %s" % (r))

            text += str(r) + ' '
            if options.verbose:
                print("Comment differs for:%s old: %s new:%s " % (detPOG, oldcert[1], newcert[1]))

            html += "<tr><th>%d</th>" % r

            for X in (oldcert, newcert):
                print("\tInfo 1: %s" % (v2c(X[0],X[1])))
                html += "<td class='%s'>%s</td>" % (v2c(X[0],X[1]), p2t(X))

            html += "<td>%s</td><td>%s</td>" % (oldcert[3], newcert[3])
            html += "<td>%s</td></tr>\n" % (note)

        else:
            if options.verbose:
                print("Run with same flag")
            if options.allrun:
                html += "<tr><th>%d</th>" % (r)
                for X in (oldcert, newcert):
                    html += "<td class='%s'>%s</td>" % (v2c(X[0],X[1]), p2t(X))

                html += "<td>%s</td><td>%s</td>" % (oldcert[3], newcert[3])
                html += "<td>%s</td></tr>\n" % (note)

    html += "</table></body></html>"
    html += "</br>"

    # Writing list of runs with different flags in a text file
    if text:
        html += "Run(-s) with different flags: %s </br><hr>" % (text)
        if options.verbose:
            print("\tDifference")

    else:
        if options.verbose:
            print("\tNo difference in flag")
        html += "No difference found in certification for %s <hr>" % (detPOG)
        text += 'No difference found!!!'

    if options.verbose:
        print("Creating file with different flags: %s"  % ("DiffRunsFor%s.txt" % (detPOG)))

    with open("DiffRunsFor%s.txt" % (detPOG), "w") as diffrun:
        diffrun.write(text)

    return html

if __name__ == '__main__':
    html = ""
    notes = {}
    runlist = {}
    lumiCache = {}

    parser = argparse.ArgumentParser(
                description='Compare global runRegistry workspace to each subdetectors localws',
                formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-m", "--min", dest="min", type=int, default=271000,
            help="Minimum run")
    parser.add_argument("-M", "--max", dest="max", type=int, default=999999,
            help="Maximum run")
    parser.add_argument("--min-ls", dest="minls", type=int, default=10,
            help="Ignore runs with less than X lumis")
    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true",
            default=False, help="Print more info")
    parser.add_argument("-n", "--notes", dest="notes", type=str,
            default="notes.txt", help="Text file with notes")
    parser.add_argument("-p", "--process", dest="process", type=str,
            default="PromptReco", help="Dataset name for last certification")
    parser.add_argument("-r", "--refprocess", dest="refprocess", type=str,
            default="PromptReco", help="Dataset name to be used as reference")
    parser.add_argument("-g", "--group", dest="group", type=str,
            default="Collisions18", help="Text file with run list")
    parser.add_argument("-i", "--infile", dest="infile", type=str,
            default="Collisions18.txt", help="Text file with run list")
    parser.add_argument("-a", "--allrun", dest="allrun", action="store_true",
            default=True, help="Show all runs in the table")

    options = parser.parse_args()

    query = "select d.RUN_NUMBER, d.RDA_STATE from runreg_global.datasets_off d where "

    run_query = "d.RUN_NUMBER >= %s AND d.RUN_NUMBER <= %s" % (options.min, options.max)

    if options.infile:
        run_query = ""
        filename = options.infile
        inputfile = open(filename,"r")
        print("Opening file %s which contains the run list" % (filename))
        selruns = [file.strip() for file in open(filename)]
        if options.verbose:
            print("Changing run selection to selected runs")
        for run in selruns:
            if options.verbose:
                print(run)
            run_query += "d.RUN_NUMBER = %s OR " % (run)
        #remove last OR from select query
        run_query = run_query[:-3]

    print("Run selection: %s"  % (run_query))
    if options.verbose:
        print("rhub query: %s" % (query + run_query))

    if options.notes:
        try:
            nfile = open(options.notes, "r")
            for l in nfile:
                m = re.match(r"\s*(\d+)\s*:?\s+(.*)", l)
                if m:
                    notes[int(m.group(1))] = m.group(2)
        except IOError:
            print("Couldn't read notes file %s" % (options.notes))

    lumiCacheName = "lumi-by-run-grin.txt"

    if options.verbose:
        print("lumiCacheName: %s" % (lumiCacheName))

    try:
        lumiFile = open(lumiCacheName, "r")
        print("Opening file: %s" % (lumiCacheName))
        for l in lumiFile:
            m = re.match(r"(\d+)\s+(\d+)\s+([0-9.]+).*", l)
            if m:
                lumiCache[int(m.group(1))] = int(m.group(2)), float(m.group(3))
    except IOError:
       pass

    html = """
    <html>
    <head>
    <title>Certification Status, %s (%s)</title>
      <style type='text/css'>
        body { font-family: "Candara", sans-serif; text-align: center;}
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
        table, tr { background-color: black; margin: 0px auto;}
      </style>
    </head>
    <body>
    <h1>Certification Status, %s (%s)</h1>
    <hr>
    """ % (options.group, time.ctime(), options.group, time.ctime())

    listDETPOG = ['CSC', 'DT', 'ECAL', 'ES', 'HCAL', 'HLT', 'L1tmu', 'L1tcalo', 'LUMI',
        'PIX', 'RPC', 'STRIP', 'EGAMMA', 'JETMET', 'MUON', 'TRACK']

    map_DB_to_column = {"CSC": {"workspace": "CSC",
                                "DB_column": "RDA_CMP_CSC"},
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

    ds_query_global = "AND "
    ds_query_local = "AND "

    if "OR" in options.refprocess:
        # some conversion for inline OR with old-format
        for i in options.refprocess.split("OR"):
            ds_query_global += "d.RDA_NAME = %s OR " % (i)
    else:
        ds_query_global = "d.RDA_NAME like '%%%s%%'" % (options.refprocess)

    if "OR" in options.process:
        # some conversion for inline OR with old-format
        for i in options.process.split("OR"):
            ds_query_local += "d.RDA_NAME = %s OR " % (i)
    else:
        ds_query_local = "d.RDA_NAME like '%%%s%%'" % (options.process)

    for subdetector in listDETPOG:
        if options.verbose:
            print("\tChecking: %s" % (subdetector))

        print("local workspace: %s" % (map_DB_to_column[subdetector.upper()]["workspace"]))
        html += "<h3>Certification Status for %s </h3>" % (subdetector)

        global_query = ("select d.RUN_NUMBER, d.RDA_STATE, d.RDA_CMP_%s, d.RDA_CMP_%s_COMMENT "
                "from runreg_global.datasets_off d where "
                "d.RUN_CLASS_NAME like '%%%s%%' AND (d.RDA_STATE = 'OPEN' "
                "OR d.RDA_STATE = 'SIGNOFF' OR d.RDA_STATE = 'COMPLETED') AND %s AND (%s)") % (
                    subdetector, subdetector, options.group, ds_query_global, run_query)


        local_query = ("select d.RUN_NUMBER, d.RDA_STATE, d.%s, d.%s_COMMENT "
                "from runreg_%s.datasets_off d where "
                "d.RUN_CLASS_NAME like '%%%s%%' AND (d.RDA_STATE = 'OPEN' "
                "OR d.RDA_STATE = 'SIGNOFF' OR d.RDA_STATE = 'COMPLETED') AND %s AND (%s)") % (
                        map_DB_to_column[subdetector.upper()]["DB_column"],
                        map_DB_to_column[subdetector.upper()]["DB_column"],
                        map_DB_to_column[subdetector.upper()]["workspace"],
                        options.group, ds_query_local, run_query)

        if options.verbose:
            print("global query: %s" % (global_query))
            print("local query: %s" % (local_query))

        global_res = query_runregistry(global_query)
        local_res = query_runregistry(local_query)

        # parse subdetectors global column data into GLOBAL runlist object
        for el in global_res:
            if el[0] not in runlist:
                runlist[el[0]] = {}

            runlist[el[0]]["RR_%s_GLOBAL_%s" % (subdetector, options.refprocess)] = [
                    el[1]  == "OPEN",
                    "Good" if el[2] == "GOOD" else el[2],
                    el[3],
                    el[1]]

        # do same for local workspace
        for el in local_res:
            if el[0] not in runlist:
                runlist[el[0]] = {}

            runlist[el[0]]["RR_%s_LOCAL_%s" % (subdetector, options.process)] = [
                    el[1]  == "OPEN",
                    "Good" if el[2] == "GOOD" else el[2],
                    el[3],
                    el[1]]

        html += createhtmlinfo(runlist, subdetector)

    print("Done")

    html += "</body></html>"
    certday = date.today().strftime("%Y%m%d")
    #out = open("/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions17/13TeV/RRComparison/status.%s.%s.html" % (groupName, certday), "w")
    out = open("status_new.%s.html" % options.group, "w")
    out.write(html.encode('utf-8'))
    out.close()
