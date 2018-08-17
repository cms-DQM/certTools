#!/usr/bin/env python

#
# How to run the script: since information from the GUI can be accessed only using a 
# valid grid certificate, you need to do the following step:
# voms-proxy-init then write your GRID password, taking note of the location where the proxy is created (here as ex. /tmp/x509up_zzzz)
# setenv X509_USER_PROXY /tmp/x509up_zzzz
# python CheckCosmicTrack --min=xxx --max=yyy --who=YourName > CosmicTrackNum.txt
#
# VA run this once per week: voms-proxy-init --valid 168:0 --out /afs/cern.ch/user/a/azzolini/proxyVIR
#
import os, os.path, sys, re, urllib2, socket, traceback, errno, thread, hmac, hashlib
import commands

from optparse import OptionParser
parser = OptionParser()
parser.add_option("-m", "--min", dest="min", type="int", default=268000, help="Minimum run")
parser.add_option("-M", "--max", dest="max", type="int", default=999999, help="Maximum run")
parser.add_option("-p", "--process", dest="process", default="Run2016A-PromptReco-v1", help="Era and processing to consider")
parser.add_option("-o", "--out", dest="out", default="test.root", help="Output file")
parser.add_option("-w", "--who",dest="who", default="VA",help="run in batch or not")
(options, args) = parser.parse_args()

# VA testing possibility to run in batch
if options.who:
    whoami=options.who
    if whoami == "VA":
        os.environ["X509_USER_PROXY"] = "/afs/cern.ch/user/a/azzolini/proxyVIR"
        print os.environ["X509_USER_PROXY"]
    elif whoami == "AK":
        os.environ["X509_USER_PROXY"] = "/afs/cern.ch/user/a/amkalsi/proxyVIR"
        print os.environ["X509_USER_PROXY"]
    else: 
        print " do you have a valid proxy? did you run 'voms-proxy-init ' and then 'setenv X509_USER_PROXY /tmp/x509up_zzzz?" 

#os.putenv["X509_USER_PROXY", "/afs/cern.ch/user/a/azzolini/x509up_u36282"]
#os.system("echo $X509_USER_PROXY")
#-----------------------------


from rrapi import RRApi, RRApiError

from dqmjson import *
#from optparse import OptionParser

URL = 'http://runregistry.web.cern.ch/runregistry/'
api = RRApi(URL, debug = False)




# Primary Dataset
pds = ['Cosmics']

#run = [x.strip() for x in open(args[0])]
#run.sort()
run = []
cosmicsList = []
cosmicsShortList = []
cosmicsNotYetDQMGUI = []

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
                                      'datasetState': "'OPEN'", \
                                      "run": {"rowClass": "org.cern.cms.dqm.runregistry.user.model.RunSummaryRowGlobal", \
                                      "filter": {"bfield": "> 3.7"}}})
#                                      'datasetState': "<> 'COMPLETED'", \

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
    if runnum > "281214" and run <= "284024":
        process = "Run2016H-PromptReco-v2"
    if runnum > "287178" and run <= "294640":
        process = "Commissioning2017-PromptReco-v1"
    if runnum > "294645" and run <= "296165":
        process = "Run2017A-PromptReco-v1"
    if runnum > "296166" and run <= "296661":
        process = "Run2017A-PromptReco-v2"
    if runnum > "296662" and run <= "296985":
        process = "Run2017A-PromptReco-v3"
    if runnum > "297033" and run <= "298397":
        process = "Run2017B-PromptReco-v1"
    if runnum > "298398" and run <= "299314":
        process = "Run2017B-PromptReco-v2"
    if runnum > "299336" and run <= "299917":
        process = "Run2017C-PromptReco-v1"
    if runnum > "299918" and run <= "300702":
        process = "Run2017C-PromptReco-v2"
    if runnum > "300703" and run <= "302029":
        process = "Run2017C-PromptReco-v3"
    if runnum > "302030" and run <= "302663":
        process = "Run2017D-PromptReco-v1"
    if runnum > "302664" and run <= "304819":
        process = "Run2017E-PromptReco-v1"
    if runnum > "304911" and run <= "306462":
        process = "Run2017F-PromptReco-v1"
    if runnum > "306463" and run <= "306826":
        process = "Run2017G-PromptReco-v1"
    if runnum > "306828" and run <= "307080":
        process = "Run2017H-PromptReco-v1"
    if runnum > "308327" and run <= "315250":
        process = "Commissioning2018-PromptReco-v1"
    if runnum > "315260" and run <= "316181":
        process = "Run2018A-PromptReco-v1"
    if runnum > "316246" and run <= "316976":
        process = "Run2018A-PromptReco-v2"
    if runnum > "316998" and run <= "317943":
        process = "Run2018B-PromptReco-v1"
    if runnum > "318046" and run <= "319308":
        process = "Run2018B-PromptReco-v2"
    if runnum > "319313" and run <= "319413":
        process = "Run2018C-PromptReco-v1"
    if runnum > "319415" and run <= "319823":
        process = "Run2018C-PromptReco-v2"
    if runnum > "319826" and run <= "320239":
        process = "Run2018C-PromptReco-v3"

    #print "serverurl", serverurl
    tk = dqm_get_json(serverurl, runnum, "/Cosmics/%s/DQMIO" % process, "Tracking/TrackParameters/GeneralProperties", rootContent=True)
    #print runnum
    if not tk.keys():
        print "---------->  run not in DQM or Tracking plot do not exist", runnum
        cosmicsNotYetDQMGUI.append(runnum)
        continue
    else:
        #print runnum
        #print tk.keys()
        ntracks = tk['TrackPt_CKFTk']['nentries']


    if ntracks > 100:
        cosmicsList.append(runnum)
        print runnum, ntracks
    else:
        cosmicsShortList.append(runnum)
        print "---------->  missed:", runnum, ntracks


#NumRun = len(cosmicsList)
cosmicsList.sort()
print "\n \n final %s list to be certified:" % (len(cosmicsList))
print " ".join(cosmicsList)

cosmicsShortList.sort()
print "\n \n These %s runs will NOT be certified : check if they have correctly not enough tracks:" % (len(cosmicsShortList))
print " ".join(cosmicsShortList)

cosmicsNotYetDQMGUI.sort()
print "\n \n These %s runs cannot be considered becasue not yet in DQM GUI" % (len(cosmicsNotYetDQMGUI))
print " ".join(cosmicsNotYetDQMGUI) 

with open("/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/scripts/CMSSW_10_0_4/scripts/CosmicsToBeCertified.txt", "w") as f:
    for s in cosmicsList:
#        f.write(str(s)+"\t")
        f.write(str(s)+"\n")
print "\n CosmicsToBeCertified.txt has been updated."
