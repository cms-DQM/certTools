from optparse import OptionParser
from rrapi import RRApi, RRApiError
import sys

##Run classification
groupName = "Collisions12"
#groupName = "Cosmics12"
prompt = ['like Prompt']
#prompt = ['Collisions']

parser = OptionParser()
parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False)
parser.add_option("-m", "--min", dest="min", type="int", default=190456, help="Minimum run")
parser.add_option("-M", "--max", dest="max", type="int", default=999999, help="Maximum run")
parser.add_option("-s", "--state", dest="state", type="choice", \
                  default="SIGNOFF", \
                  help="State for run list creation: use SIGNOFF, COMPLETED.", \
                  choices=('SIGNOFF', 'COMPLETED'))
(options, args) = parser.parse_args()

runlist=[]

URL = 'http://runregistry.web.cern.ch/runregistry/'
api = RRApi(URL, debug = True)

def getRR(whichRR, dataName, groupName, options):
    print "Querying %s RunRegistry for %s runs...\n" % (whichRR, dataName)
    mycolumns = ['runNumber']
    ##Query RR
    if api.app == "user": 
        if dataName == "Online":
            runs = api.data(workspace = whichRR, table = 'datasets', \
                            template = 'csv', columns = mycolumns, \
                            filter = {'runNumber': '>= %d and <= %d' % \
                                      (options.min, options.max), \
                                      'runClassName': "like '%%%s%%'" % groupName, \
                                      'datasetName': "like '%%%s%%'" % dataName })
            if options.verbose:
                print "Online query ", runs
            if not "runNumber" in runs:
                if options.verbose:
                    print "Failed RR query for Online runs"
                    sys.exit(1)
        if dataName == "Prompt":
            runs = api.data(workspace = whichRR, table = 'datasets', \
                            template = 'csv', columns = mycolumns, \
                            filter = {'runNumber':'>= %d and <= %d' % \
                                      (options.min, options.max), \
                                      'runClassName': "like '%%%s%%'" % groupName, \
                                      'datasetName': "like '%%%s%%'" % dataName, \
                                      'datasetState': '= %s' % options.state})
            if options.verbose:
                print "Offline query", runs
            if not "runNumber" in runs:
                print "Failed RR query for Offline runs"
                sys.exit(1)
        return [run for run in runs.split('\n') if run.isdigit()]


##Start running RR queries
runonline  = getRR("GLOBAL", "Online", groupName, options)
runoffline = getRR("GLOBAL", "Prompt", groupName, options)

runlist = [run for run in runonline if run in runoffline]
runlist.sort()
print "\n\nRuns in %s state:" % options.state
print " ".join(runlist)

runlistcomp = [run for run in runonline if run not in runoffline]
runlistcomp.sort()
print "\n\nRuns NOT in %s state:" % options.state
print " ".join(runlistcomp)
