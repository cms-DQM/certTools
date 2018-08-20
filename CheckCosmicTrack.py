#!/usr/bin/env python

# How to run the script: since information from the GUI can be accessed only using a
# valid grid certificate, you need to do the following step:
# voms-proxy-init then write your GRID password, taking note of the location where the proxy is created (here as ex. /tmp/x509up_zzzz)
# setenv X509_USER_PROXY /tmp/x509up_zzzz
# python CheckCosmicTrack --min=xxx --max=yyy --who=YourName > CosmicTrackNum.txt
#
# VA run this once per week: voms-proxy-init --valid 168:0 --out /afs/cern.ch/user/a/azzolini/proxyVIR

import os
import re
import sys
import argparse

from rhapi import RhApi
from dqmjson import *

def query_runregistry(query):
    """
    query run registry's tables using resthub API
    """
    try:
        api = RhApi("http://vocms00170:2113", debug=False)
        rr_data = api.json_all(query, inline_clobs=True)
    except Exception as ex:
        print("Error while using RestHub API: %s" % (ex))
        sys.exit(-1)

    return rr_data

if __name__ == '__main__':
    # Primary Dataset
    pds = ['Cosmics']

    run = []
    cosmicsList = []
    cosmicsShortList = []
    cosmicsNotYetDQMGUI = []

    parser = argparse.ArgumentParser(
            description='Get a list of Cosmics runs to certify',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-m", "--min", dest="min", type=int, default=268000,
            help="Minimum run")
    parser.add_argument("-M", "--max", dest="max", type=int, default=999999,
            help="Maximum run")
    parser.add_argument("-p", "--process", dest="process", default="Run2016A-PromptReco-v1",
            help="Era and processing to consider")
    parser.add_argument("-o", "--out", dest="out", default="test.root",
            help="Output file")
    parser.add_argument("-w", "--who",dest="who", default="VA",
            help="run in batch or not")

    options = parser.parse_args()

    # VA testing possibility to run in batch
    if options.who:
        whoami = options.who
        if whoami == "VA":
            os.environ["X509_USER_PROXY"] = "/afs/cern.ch/user/a/azzolini/proxyVIR"
            print(os.environ["X509_USER_PROXY"])
        elif whoami == "AK":
            os.environ["X509_USER_PROXY"] = "/afs/cern.ch/user/a/amkalsi/proxyVIR"
            print(os.environ["X509_USER_PROXY"])
        else:
            print(os.environ["X509_USER_PROXY"])
            print("Do you have a valid proxy? did you run 'voms-proxy-init ' "
                "and then 'setenv X509_USER_PROXY /tmp/x509up_zzzz?")

    folder = '/Tracking/TrackParameters/GeneralProperties'

    # we always query for PromptReco/Cos if not it would have failed before!
    dataNameFilter = 'PromptReco/Cos'
    query = "select d.RUN_NUMBER from runreg_global.datasets_off d, runreg_global.runs_off r where "
    query += "d.RUN_CLASS_NAME like '%Cosmics18%' AND d.RDA_STATE = 'OPEN' "
    query += "AND d.RDA_NAME like '%%%s%%' AND d.RUN_NUMBER >= %s AND d.RUN_NUMBER <= %s " % (
            dataNameFilter, options.min,  options.max)

    query += "AND r.BFIELD > 3.7 AND r.RUNNUMBER = d.RUN_NUMBER"

    data = query_runregistry(query)
    # converting data structure
    run = [str(el[0]) for el in data]
    run.sort()
    print("Fetching %s runs, please be patient" % (len(run)))

    to_match = re.compile("/[\w\d]+/[\w\d]+-PromptReco-v[\d]/DQMIO")
    process = options.process

    # we want to re-use x509 opener
    opener = build_opener()

    ## query DBSReader for each run and get it's full PromtReco processed name
    for runnum in run:
        datasetlist = query_DBS(opener,
                "https://cmsweb.cern.ch/dbs/prod/global/DBSReader/datasets",
                "run_num=%s" % (runnum))

        for el in datasetlist:
            if re.match(to_match, el["dataset"]):
                print("for run: %s we match this dataset:%s " % (runnum, el["dataset"]))
                process = el["dataset"].split("/")[2]
                #if we match this means we don't need to check other datasets
                break

        serverurl = 'https://cmsweb.cern.ch/dqm/offline'
        ident = "DQMToJson/1.0 python/%d.%d.%d" % sys.version_info[:3]
        tk = dqm_get_json(serverurl, runnum, "/Cosmics/%s/DQMIO" % process,
                "Tracking/TrackParameters/GeneralProperties", rootContent=True, ident=ident)

        if not tk.keys():
            print("--> run: %s not in DQM or Tracking plot do not exist" % (runnum))
            cosmicsNotYetDQMGUI.append(runnum)
            continue
        else:
            ntracks = tk['TrackPt_CKFTk']['nentries']

        if ntracks > 100:
            cosmicsList.append(runnum)
            print(runnum, ntracks)
        else:
            cosmicsShortList.append(runnum)
            print("--> missed: %s %s" % (runnum, ntracks))

    cosmicsList.sort()
    print("\n\n final %s list to be certified: %s" % (len(cosmicsList), " ".join(cosmicsList)))

    cosmicsShortList.sort()
    print("\n\n These %s runs will NOT be certified: check if they have correctly not enough tracks: %s" % (
            len(cosmicsShortList), " ".join(cosmicsShortList)))

    cosmicsNotYetDQMGUI.sort()
    print("\n\n These %s runs cannot be considered becasue not yet in DQM GUI: %s" % (
            len(cosmicsNotYetDQMGUI), " ".join(cosmicsNotYetDQMGUI)))

    #with open("/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/scripts/CMSSW_10_0_4/scripts/CosmicsToBeCertified.txt", "w") as f:
    with open("CosmicsToBeCertified.txt", "w") as f:
        for s in cosmicsList:
            f.write(str(s)+"\n")

    print("CosmicsToBeCertified.txt has been updated")
