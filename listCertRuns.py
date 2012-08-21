from optparse import OptionParser
from rrapi import RRApi, RRApiError
import sys

# Parse Options
parser = OptionParser()
parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False)
parser.add_option("-m", "--min", dest="min", type="int", default=190456, help="Minimum run")
parser.add_option("-M", "--max", dest="max", type="int", default=999999, help="Maximum run")
parser.add_option("-g", "--group", dest="group", type="string", \
                  default="Collisions12", \
                  help="Group Name, for instance 'Collisions12', 'Cosmics12'")
(options, args) = parser.parse_args()

#Run classification
groupName = options.group


runlist=[]

URL = 'http://runregistry.web.cern.ch/runregistry/'
api = RRApi(URL, debug = True)

def getRR(whichRR, dataName, groupName, options, state=''):
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
            dataNameFilter = 'PromptReco/Co'
            runs = api.data(workspace = whichRR, table = 'datasets', \
                            template = 'csv', columns = mycolumns, \
                            filter = {'runNumber':'>= %d and <= %d' % \
                                      (options.min, options.max), \
                                      'runClassName': "like '%%%s%%'" % groupName, \
                                      'datasetName': "like '%%%s%%'" % dataNameFilter, \
                                      'datasetState': '= %s' % state})
            if options.verbose:
                print "Offline query", runs
            if not "runNumber" in runs:
                print "Failed RR query for Offline runs"
                sys.exit(1)
        return [run for run in runs.split('\n') if run.isdigit()]


##Start running RR queries
runonline    = getRR("GLOBAL", "Online", groupName, options, '')
runoffline   = getRR("GLOBAL", "Prompt", groupName, options, '')
runcompleted = getRR("GLOBAL", "Prompt", groupName, options, 'COMPLETED')
runsignoff    = getRR("GLOBAL", "Prompt", groupName, options, 'SIGNOFF')
runopen      = getRR("GLOBAL", "Prompt", groupName, options, 'OPEN')


runonline.sort()
print "\n\nThere are %s %s runs in Online RR:" % (len(runonline), groupName)
print " ".join(runonline)

runoffline.sort()
print "\n\nThere are %s %s runs in Offline RR:" % (len(runoffline), groupName)
print " ".join(runoffline)

runofflonly = [run for run in runonline if run not in runoffline]
runofflonly.sort()
print "\n\nThere are %s %s runs in Online RR which are not in Offline RR:" % (len(runofflonly), groupName)
print " ".join(runofflonly)


runcompleted.sort()
print "\n\nThere are %s %s runs in 'COMPLETED' state in Offline RR:" % (len(runcompleted), groupName)
print " ".join(runcompleted)

runsignoff.sort()
print "\n\nThere are %s %s runs in 'SIGNOFF' state in Offline RR:" % (len(runsignoff), groupName)
print " ".join(runsignoff)

runopen.sort()
print "\n\nThere are %s %s runs in 'OPEN' state in Offline RR:" % (len(runopen), groupName)
print " ".join(runopen)





