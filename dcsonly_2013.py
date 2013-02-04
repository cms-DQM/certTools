#!/usr/bin/env python26
import os,string,sys,commands,time,json
from rrapi import RRApi, RRApiError

def toOrdinaryJSON(fromRR3, verbose=False):
    result = {}
    for block in fromRR3:
        if len(block) == 3:
            runNum = block['runNumber']
            lumiStart = block['sectionFrom']
            lumiEnd = block['sectionTo']
            if verbose:
                print " debug: Run ", runNum, " Lumi ", lumiStart, ", ", lumiEnd
            result.setdefault(str(runNum), []).append([lumiStart, lumiEnd])
    return result

def getRunList(minRun, save=False):

    runlist = []
    FULLADDRESS  = "http://runregistry.web.cern.ch/runregistry/"

    print "RunRegistry from: ",FULLADDRESS
    try:
        api = RRApi(FULLADDRESS, debug = True)
    except RRApiError, e:
        print e

#old filter (no det active)
#    filter = {"runNumber": ">= %d" % minRun, "dataset": {"rowClass": "org.cern.cms.dqm.runregistry.user.model.RunDatasetRowGlobal", "filter": {"online": "= true", "datasetName": "like %Online%ALL", "runClassName" : "Collisions12", "run": {"rowClass": "org.cern.cms.dqm.runregistry.user.model.RunSummaryRowGlobal", "filter": {"bfield": "> 3.7"}}}}}
#    filter = {"runNumber": ">= %d" % minRun, "dataset": {"rowClass": "org.cern.cms.dqm.runregistry.user.model.RunDatasetRowGlobal", "filter": {"online": "= true", "datasetName": "like %Online%ALL", "runClassName" : "Collisions12", "run": {"rowClass": "org.cern.cms.dqm.runregistry.user.model.RunSummaryRowGlobal", "filter": {"bfield": "> 3.7", "pixelPresent": "= true", "trackerPresent": "= true"}}}}}
    filter = {"runNumber": ">= %d" % minRun, "dataset": {"rowClass": "org.cern.cms.dqm.runregistry.user.model.RunDatasetRowGlobal", "filter": {"online": "= true", "datasetName": "like %Online%ALL", "runClassName" : "Collisions13", "run": {"rowClass": "org.cern.cms.dqm.runregistry.user.model.RunSummaryRowGlobal", "filter": {"bfield": "> 3.7", "pixelPresent": "= true", "trackerPresent": "= true"}}}}}

    filter.setdefault("fpixReady", "isNull OR = true")
    filter.setdefault("bpixReady", "isNull OR = true")
    filter.setdefault("tecmReady", "isNull OR = true")
    filter.setdefault("tecpReady", "isNull OR = true")
    filter.setdefault("tobReady", "isNull OR = true")
    filter.setdefault("tibtidReady", "isNull OR = true")
    filter.setdefault("cmsActive", "isNull OR = true")
    template = 'json'
    table = 'datasetlumis'
    print json.dumps(filter)
    dcs_only = api.data(workspace = 'GLOBAL', table = table, template = template, columns = ['runNumber', 'sectionFrom', 'sectionTo'], filter = filter)

    print json.dumps(dcs_only, indent=2)

    print json.dumps(toOrdinaryJSON(dcs_only, verbose=False), indent=2)
    print len(dcs_only)

    if len(dcs_only)!=0:
        if save:
            # lumiSummary = open('/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions12/8TeV/DCSOnly/json_DCSONLY.txt', 'w')
            lumiSummary = open('/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions13/pPb/DCSOnly/json_DCSONLY.txt', 'w')
            # lumiSummary = open('./json_DCSONLY.txt', 'w')
            json.dump(toOrdinaryJSON(dcs_only, verbose=False), lumiSummary, indent=2, sort_keys=True)
            lumiSummary.close()
    else:
        print " Something wrong, the DCSONLY file has 0 length... skipping it"
                        
#     for line in run_data.split("\n"):
#         #print line
#         run=line.split(',')[0]
#         if "RUN_NUMBER" in run or run == "":
#             continue
#         #print "RUN: " + run
#         runlist.append(int(run))
#     return runlist

# getRunList(190389, save=True)
getRunList(210498, save=True)

