from optparse import OptionParser
from rrapi import RRApi, RRApiError
import sys, commands

# Parse Options
parser = OptionParser()
parser.add_option("-v", "--verbose", dest="verbose", action="store_true", default=False)
parser.add_option("-m", "--min", dest="min", type="int", default=268000, help="Minimum run")
parser.add_option("-M", "--max", dest="max", type="int", default=999999, help="Maximum run")
parser.add_option("-g", "--group", dest="group", type="string", \
                  default="Collisions16", \
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

runtobecertified = runsignoff + runopen + runofflonly 
# Check the luminosity to remove short runs
print " ".join(runtobecertified)
for run in runtobecertified:
    print run
    command='brilcalc lumi -r %s -o stdout' %run
    if options.verbose:
        print command
    (status, out) = commands.getstatusoutput(command)

    if status:
        sys.stderr.write(out)
        print "\nERROR on brilcalc command: %s\nDid you setup the variable for brilcalc?" % command
        print "export PATH=$HOME/.local/bin:/afs/cern.ch/cms/lumi/brilconda-1.0.3/bin:$PATH"
        sys.exit(1)

    for line in open('stdout'):
        li=line.strip()
        if not li.startswith("#"):
            reclumi = float(line.rpartition(',')[2])/100.
            if options.verbose: print "Recorded lumi for run %s is %f" % (run, reclumi)
            if (reclumi - 80.0) >= 0.0:
                if options.verbose: print "Run is long enough"
            else:
                if options.verbose: print "Run %s is short and it should be removed from the certification list" %run
                runtobecertified.remove(run)

print "\n------ There are %s %s runs to be certified this week -----" % (len(runtobecertified), groupName)
print " ".join(runtobecertified)

