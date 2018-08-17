#!/usr/bin/env python

#
# How to run the script: since information from the GUI can be accessed only using a 
# valid grid certificate, you need to do the following step:
# voms-proxy-init then write your password
# setenv X509_USER_PROXY /tmp/x509up_zzzz
# python CheckCosmicTrack.py --min=xxx --max=yyy > CosmicTrackNum.txt
#
import os, os.path, sys, re, urllib2, socket, traceback, errno, thread, hmac, hashlib
import commands

from rrapi import RRApi, RRApiError

from dqmjson import *
from optparse import OptionParser

URL = 'http://runregistry.web.cern.ch/runregistry/'
api = RRApi(URL, debug = False)

parser = OptionParser()
parser.add_option("-m", "--min", dest="min", type="int", default=268000, help="Minimum run")
parser.add_option("-M", "--max", dest="max", type="int", default=999999, help="Maximum run")
parser.add_option("-p", "--process", dest="process", default="Run2016A-PromptReco-v1", help="Era and processing to consider")
parser.add_option("-o", "--out", dest="out", default="test.root", help="Output file")
(options, args) = parser.parse_args()

# Primary Dataset
pds = ['Cosmics']

#run = [x.strip() for x in open(args[0])]
#run.sort()
run = []

def getRR(whichRR, dataName, groupName, options, state=''):
    print "Querying %s RunRegistry for %s runs...\n" % (whichRR, dataName)
    mycolumns = ['runNumber']
    ##Query RR
    if api.app == "user": 
        if dataName == "Prompt":
            dataNameFilter = 'PromptReco/Cos'
            runs = api.data(workspace = whichRR, table = 'datasets', \
                            template = 'csv', columns = mycolumns, \
                            filter = {'runNumber':'>= %d and <= %d' % \
                                      (options.min, options.max), \
                                      'runClassName': "like 'Cosmics18'" , \
                                      'datasetName': "like '%%%s%%'" % dataNameFilter, \
                                      'datasetState': "<> 'COMPLETED'", \
                                      "run": {"rowClass": "org.cern.cms.dqm.runregistry.user.model.RunSummaryRowGlobal", \
                                      "filter": {"bfield": "> 3.7"}}})
            if not "runNumber" in runs:
                print "Failed RR query for Offline runs"
                sys.exit(1)
        return [run for run in runs.split('\n') if run.isdigit()]

run = getRR("GLOBAL", "Prompt", "Cosmics18", options, '')

folder = '/Tracking/TrackParameters/GeneralProperties'

print "Fetching", len(run), "runs, please be patient"

for runnum in run:
    if runnum >= "270988" and run <= "271489":
        process = "Run2016A-PromptReco-v2"
    if runnum >= "271499" and run <= "271843":
        process = "Run2016A-PromptReco-v1"
    if runnum >= "271861" and run <= "273148":
        process = "Run2016B-PromptReco-v1"
    if runnum >= "273163" and run <= "275418":
        process = "Run2016B-PromptReco-v2"
    if runnum >= "275419" and run <= "276310":
        process = "Run2016C-PromptReco-v2"
    if runnum > "276333" and run <= "276804":
        process = "Run2016D-PromptReco-v2"
    if runnum > "276825" and run <= "277754":
        process = "Run2016E-PromptReco-v2"
    if runnum > "277772" and run <= "278798":
        process = "Run2016F-PromptReco-v1"
    if runnum > "278809" and run <= "280831":
        process = "Run2016G-PromptReco-v1"
    if runnum > "280992" and run <= "281199":
        process = "Run2016H-PromptReco-v1"
    if runnum > "281214" and run <= "999999":
        process = "Run2016H-PromptReco-v2"

    tk = dqm_get_json(serverurl, runnum, "/Cosmics/%s/DQMIO" % process, "Tracking/TrackParameters/GeneralProperties", rootContent=True)
    ntracks = tk['TrackPt_CKFTk']['nentries']

    print runnum, ntracks



