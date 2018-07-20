#!/usr/bin/env python

import sys,ConfigParser,os,string,commands,time,xmlrpclib

from optparse import OptionParser
import json

from rrapi import RRApi, RRApiError

parser=OptionParser()
parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False)
parser.add_option("-m", "--min", dest="min", type="int", default=294000, help="Minimum run")
parser.add_option("-M", "--max", dest="max", type="int", default=999999, help="Maximum run")
parser.add_option("-i", "--infile", dest="infile", type="string", default="",
    help="Text file with run list")
(options, args) = parser.parse_args()

RUNMINCFG=options.min
RUNMAXCFG=options.max

runsel = ""
runsel = '>= %d and <= %d'%(options.min,options.max)

if options.infile:
    runsel = ""
    filename=options.infile
    inputfile = open(filename,"r")
    print "Opening file ", filename, "which contains the run list"
    selruns = [file.strip() for file in open(filename)]
    for run in selruns:
        print run
        runsel += " OR = " + run

if options.verbose: print "Run selection ", runsel

URL  = "http://runregistry.web.cern.ch/runregistry/"
api = RRApi(URL, debug = True)
RUN_DATA = api.data(workspace = 'GLOBAL', table = 'runsummary', template = 'tsv', columns = ['number','events','bfield','hltKeyDescription'], filter = {"runClassName": "Collisions18", "number": '%s' % runsel, "datasets": {"rowClass": "org.cern.cms.dqm.runregistry.user.model.RunDatasetRowGlobal", "datasetName": "like %Online%"}}, tag= 'LATEST')

print RUN_DATA
