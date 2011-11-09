#!/usr/bin/env python
# Valdas/Vir date: 05/07/2011 --> fixed bug that shows up if LSCOMMENT=True and (RunMax - RunMin) > 0.(see line 94)
# LM: version date: 01/02/2010 --> fixed dataset search and added json output file (optional)
# LM: updated 03/04/2010 --> adapted to new runreg api (and dcs status info)
# LM: updated 15/04/2010 --> added bfield threshold
# LM: updated 3/05/2010 --> added Energy range selection,Lumi quality flags and DBS cross check
# LM: updated 1/07/2010 --> fixed a bug which would prevent the BAD LS comment to be picked up for some runs
# LM: updated 7/07/2010 --> removes empty runs from the output JSON
# LM: updated 12/7/2010 --> adapted to ne RR (H0 --> Ho)
# LM: updated 14/7/2010 --> adapted to ne RR, add Online dataset selection for dcs status
# LM: updated 17/7/2010 --> added possibility to mix datasets based on run ranges
# VA: updated 10/9/2010 --> handle new table structure in RR lumi section table

# include XML-RPC client library
# RR API uses XML-RPC webservices interface for data access
import xmlrpclib,sys,ConfigParser,os,string,commands,time,re,socket
import xml.dom.minidom
from xml.dom.minidom import Node

# for json support
try: # FUTURE: Python 2.6, prior to 2.6 requires simplejson
    import json
except:
    try:
        import simplejson as json
    except:
        print "Please use lxplus or set an environment (for example crab) with json lib available"
        sys.exit(1)

global QF_Req,ls_temp_data,QF_ALL_SYS,EXCEPTION,EXRUN

EXCEPTION=False
EXRUN=-1

def invert_intervals(intervals,min_val=1,max_val=9999):
    # first order and merge in case
    if not intervals:
        return []
    intervals=merge_intervals(intervals)
    intervals = sorted(intervals, key = lambda x: x[0])
    result = []
    if min_val==-1:
        # defin min and max
        (a,b)=intervals[0]
        min_val=a
    if max_val==-1:
        (a,b)=intervals[len(intervals)-1]
        max_val=b

    curr_min=min_val
    for (x,y) in intervals:
        if x>curr_min:
            result.append((curr_min,x-1))
        curr_min=y+1
    if curr_min<max_val:
        result.append((curr_min,max_val))

#    print min_val,max_val
    return result

def merge_intervals(intervals):
    if not intervals:
        return []
    intervals = sorted(intervals, key = lambda x: x[0])
    result = []
    (a, b) = intervals[0]
    for (x, y) in intervals[1:]:
        if x <= b:
            b = max(b, y)
        else:
            result.append((a, b))
            (a, b) = (x, y)
    result.append((a, b))
    return result

def remove_html_tags(data):
    p = re.compile(r'<.*?>')
    newdata=p.sub('', data)
    newdata=newdata.replace("&nbsp;","")
    return newdata

def remove_extra_spaces(data):
    result= re.sub(r'\s', '', data)
    return result

def searchrun(runno):
    global QF_Req,ls_temp_data,QF_ALL_SYS,EXCEPTION,EXRUN
    intervallist=[]
    selectls=""

    run_found=False

    for line in ls_temp_data.split("\n"):
        if re.match('^' + runno + ',', line):
            #        if runno in line:   it was likes until 05/07/2011
            run_found=True
#            print line
            try:
                if "%%%BAD LS INFO BEGIN%%%" in line:
                    line = re.sub('%%%BAD LS INFO END%%%\s+%%%BAD LS INFO BEGIN%%%', '', line)
                    selectls=line.split("%%%BAD LS INFO BEGIN%%%")[1]
                    selectls=selectls.split("%%%BAD LS INFO END%%%")[0]
                    selectls=remove_html_tags(selectls)
                    selectls=remove_extra_spaces(selectls)
                    #print selectls
                    for tag in QF_ALL_SYS:
                        selectls=selectls.replace(tag+":","\n"+tag+":")
                    #print selectls

                    for line in selectls.split("\n"):
                        try:
                            tag=line.split(":")[0]
                            intervals=line.split(":")[1]
                        except:
                            continue
                        if tag in QF_Req.keys():
                            if QF_Req[tag]=="GOOD":
                                for interval in intervals.split(","):
                                    if "ALL" in interval:
                                        lmin=1
                                        lmax=9999
                                    else:
                                        strmin=interval.split('-')[0]
                                        strmax=interval.split('-')[1]
                                        lmin=int(strmin)
                                        if "END" in strmax:
                                            lmax=9999
                                        else:
                                            lmax=int(strmax)
                                    intervallist.append((lmin,lmax))
            except:
                EXCEPTION=True
                EXRUN=int(runno)
    intervallist=merge_intervals(intervallist)
    # print runno, intervallist
    if not run_found:
        print "\nWARNING: run "+runno+" not found in the ls table, no LSBAD comment parsed for this run"

    return intervallist


#def ene_in_run(LISTOFRUN):

# needed by ene_in_run
global RUNMIN,RUNMAX,BFIELD
def ene_in_run(run_data):
    global RUNMIN,RUNMAX,BFIELD
    BEAM_ENE_ALL=[450.0,1380.0,3500.0]
    BEAM_ENE_DEF=3500.0
    ene_map={}
    print "Retrieving energy from the RR run table and making some check...."
    for line in run_data.split("\n"):
#        print line
        run=line.split(',')[0]
        if run.isdigit():
            # we need to do something to avoid parsing commas in comment column
            group=line.split('",')[8]
            energy=group.split(',')[0]
#            print run,line,group,energy
#            fill=group.split(',')[1]
#            print run,energy,fill
            mindiff=999999.
            if energy.isdigit():
                for be in BEAM_ENE_ALL:
                    if abs(float(energy)-be)<mindiff:
                        mindiff=abs(float(energy)-be)
            if mindiff>400.0:
                print "WARNING: Something wrong with energies in run "+run
                print "WARNING: Getting: "+energy+" from RR.Using default value of:"+str(BEAM_ENE_DEF)
                energy=str(BEAM_ENE_DEF)
            ene_map[run]=float(energy)

    return ene_map

# in order to get dbsjson files
global DBS_PDS

def get_dbsjson():
    global DBS_PDS,RUNMIN,RUNMAX
    unsorted={}
    for dataset in DBS_PDS:
        command='dbs search --query="find run,lumi where dataset='+dataset+' and run >='+str(RUNMIN)+' and run<='+str(RUNMAX)+'"'
#        print command
        (status, out) = commands.getstatusoutput(command)
        if status:    ## Error case, print the command's output to stderr and exit
            sys.stderr.write(out)
            print "\nERROR on dbs command:"+command+" ..... have you done cmsenv?"
            sys.exit(1)

        for line in out.split('\n'):
            fields=line.split()
#            print fields
            if len(fields)!=2:
                continue
            if fields[0].isdigit() and fields[1].isdigit():
                run=fields[0]
                lumi=int(fields[1])
                if run not in unsorted.keys():
                    unsorted[run]=[]
                unsorted[run].append(lumi)

#    print unsorted
    sorted={}
    for run in unsorted.keys():
        lumilist=unsorted[run]
        lumilist.sort()
        sorted[run]=lumilist
#    print sorted

    dbsjson={}
    for run in sorted.keys():
        lumilist=sorted[run]
        lumiranges=[]
        lumirange=[]
        lumirange.append(lumilist[0])
        lastlumi=lumilist[0]
        for lumi in lumilist[1:]:
            if lumi>lastlumi+1:
                # close last range, append and open new range
                lumirange.append(lastlumi)
                lumiranges.append(lumirange)
                lumirange=[]
                lumirange.append(lumi)
            lastlumi=lumi
        if len(lumirange)==1:
            lumirange.append(lastlumi)
            lumiranges.append(lumirange)
        dbsjson[run]=lumiranges

#    print dbsjson
    return dbsjson




#######################################################################################
#main starts here#

QF_Req={}
GOODRUN={}
compactList = {}

QF_ALL_SYS=["Hcal","Track","Strip","Egam","Es","Dt","Csc","Pix","Muon","Rpc","Castor","Jmet","Ecal","L1t","Hlt","Lumi","NONE"]
QF_ALL_STAT=["GOOD","BAD","EXCL","NONE"]
DCS_ALL={'Bpix':'BPix',
         'Fpix':'FPix',
         'Tibtid':'TibTid',
         'TecM':'TecM',
         'TecP':'TecP',
         'Tob':'Tob',
         'Ebminus':'EbM',
         'Ebplus':'EbP',
         'EeMinus':'EeM',
         'EePlus':'EeP',
         'EsMinus':'EsM',
         'EsPlus':'EsP',
         'HbheA':'HbHeA',
         'HbheB':'HbHeB',
         'HbheC':'HbHeC',
         'Ho':'H0',
         'Hf':'Hf',
         'Dtminus':'DtM',
         'Dtplus':'DtP',
         'Dt0':'Dt0',
         'CscMinus':'CscM',
         'CscPlus':'CscP',
         'Rpc':'Rpc',
         'Castor':'Castor',
         'NONE':'NONE'}

# reading config file
if len(sys.argv)==2:
    CONFIGFILE=sys.argv[1]
else:
    CONFIGFILE='runreg.cfg'

CONFIG = ConfigParser.ConfigParser()
print 'Reading configuration file from ',CONFIGFILE
CONFIG.read(CONFIGFILE)

CFGLIST=CONFIG.items('Common')


DATASET_ALL=CONFIG.get('Common','Dataset')

GROUP=CONFIG.get('Common','Group')
ADDRESS=CONFIG.get('Common','RunReg')
RUNMIN=CONFIG.get('Common','Runmin')
RUNMAX=CONFIG.get('Common','Runmax')
QFLAGS=CONFIG.get('Common','QFLAGS')
BFIELD=CONFIG.get('Common','BField_thr')
LSPARSE=CONFIG.get('Common','LSCOMMENT')
DCSSTAT=CONFIG.get('Common','DCS')
DCSLIST=string.split(DCSSTAT,',')

#optional item
BEAMENE="-1"
DBS_PDS_ALL=""
ONLINE_CFG="FALSE"

for item in CFGLIST:
    if "beam_ene" in item[0]:
        BEAMENE=item[1]
    if "dbs_pds" in item[0]:
        DBS_PDS_ALL=item[1]
    if "online" in item[0]:
        ONLINE_CFG=item[1]
        
DBS_PDS=DBS_PDS_ALL.split(",")

ONLINE=False
if "TRUE" in ONLINE_CFG.upper() or "1" in ONLINE_CFG.upper() or "YES" in ONLINE_CFG.upper():
    ONLINE=True
elif "FALSE" in ONLINE_CFG.upper() or "0" in ONLINE_CFG.upper() or "NO" in ONLINE_CFG.upper():
    ONLINE=False
else:
    print "Error in parsing ONLINE cfg parameter: ONLINE"
    sys.exit(1)


LSCOMMENT=True
if "TRUE" in LSPARSE.upper() or "1" in LSPARSE.upper() or "YES" in LSPARSE.upper():
    LSCOMMENT=True
elif "FALSE" in LSPARSE.upper() or "0" in LSPARSE.upper() or "NO" in LSPARSE.upper():
    LSCOMMENT=False
else:
    print "Error in parsing LSCOMMENT cfg parameter: LSPARSE"
    sys.exit(1)

QFlist=string.split(QFLAGS,',')
for QF in QFlist:
    syst=string.split(QF,":")[0]
    value=string.split(QF,":")[1]
    if syst not in QF_ALL_SYS or value not in QF_ALL_STAT:
        print "QFLAG not valid:",syst,value
        sys.exit(1)
    QF_Req[syst]=value

for dcs in DCSLIST:
    if dcs not in DCS_ALL.keys():
        print "DCS not valid:",dcs
        sys.exit(1)


JSONFILE=CONFIG.get('Common','JSONFILE')

try:
    BFIELD_float=float(BFIELD)
except:
    print "BFIELD threshold value not understood:",BFIELD
    sys.exit(1)

try:
    BEAMENE_float=float(BEAMENE)
except:
    print "BEAMENE value not understood:",BEAMENE
    sys.exit(1)


# act on dataset and create run map
DS_RUNMIN=[]
DS_RUNMAX=[]
DS_LIST=[]
DATASETS=DATASET_ALL.split()

for DS in DATASETS:
    DS_TEMP=DS.split(":")
    if len(DS_TEMP)==2:
        # it asks for a run range
        dsname=DS_TEMP[0]
        dsmin=DS_TEMP[1].split("-")[0]
        dsmax=DS_TEMP[1].split("-")[1]
        DS_LIST.append(dsname)
        DS_RUNMIN.append(dsmin)
        DS_RUNMAX.append(dsmax)
    elif len(DS_TEMP)==1:
        # no run range given, take the one in cfg
        DS_LIST.append(DS)
        DS_RUNMIN.append(RUNMIN)
        DS_RUNMAX.append(RUNMAX)
    else:
        print "Dataset in cfg not understood"
        sys.exit(1)

# report the request



print "You asked for the runreg info in the run range:"+RUNMIN+"-"+RUNMAX
print "for dataset: "+DATASET_ALL
print "with the following quality flags:"
for SS in QF_Req.keys():
    print SS, QF_Req[SS]
print "and with the following DCS status:"
for dcs in DCSLIST:
    print dcs
print "Manual bad LS in comment column:",LSCOMMENT

if BEAMENE=="-1":
    print "No beam energy selection required"
else:
    print "Beam energy requested:",BEAMENE

USEDBS=False
if DBS_PDS_ALL=="":
    print "No cross-check against DBS requested"
else:
    print "Cross-check requested agains DBS in PDs:",DBS_PDS
    USEDBS=True


# get handler to RR XML-RPC server
FULLADDRESS=ADDRESS+"/xmlrpc"
print "RunRegistry from: ",FULLADDRESS
server = xmlrpclib.ServerProxy(FULLADDRESS)

# build up selection in RUN table
sel_runtable=[]
sel_dstable=[]
DSStatus=""

if ONLINE :
    DSStatus=""
    print "WARNING: ONLINE mode requested --> removing the COMPLETED requirement and looking for dcstable in Online dataset" 
else:
    DSStatus="and {datasetState} = 'COMPLETED'"

for DSindex in range(0,len(DS_LIST)):
    sel_temp="{groupName} ='"+GROUP+"' "+DSStatus+" and {runNumber} >= "+DS_RUNMIN[DSindex]+" and {runNumber} <= "+DS_RUNMAX[DSindex]+" and {bfield}>"+BFIELD+" and {datasetName} LIKE '"+DS_LIST[DSindex]+"'"
#    sel_temp="{groupName} ='"+GROUP+"' and {runNumber} >= "+DS_RUNMIN[DSindex]+" and {runNumber} <= "+DS_RUNMAX[DSindex]+" and {bfield}>"+BFIELD+" and {datasetName} LIKE '"+DS_LIST[DSindex]+"'"
    sel_runtable.append(sel_temp)

###### SEE EMAIL FROM MARCO!!!!
# the lumisection selection is on the Express dataset:
    sel_dstable_temp="{groupName} ='"+GROUP+"' and {runNumber} >= "+DS_RUNMIN[DSindex]+" and {runNumber} <= "+DS_RUNMAX[DSindex]+" and {bfield}>"+BFIELD+" and {datasetName} LIKE '"+DS_LIST[DSindex]+"'"
    sel_dstable.append(sel_dstable_temp)

#-#print sel_runtable
#-#sys.exit(1)

# the lumisection selection is on the ReReco dataset:


for key in QF_Req.keys():
    if key != "Lumi" and key != "NONE" and QF_Req[key]!="NONE":
        for DSindex in range(0,len(DS_LIST)):
            sel_runtable[DSindex]+=" and {cmp"+key+"} = '"+QF_Req[key]+"'"

#-#
#for DSindex in range(0,len(DS_LIST)):
#    print sel_runtable[DSindex]

# build up selection in RUNLUMISECTION table, not requestuing bfield here because only runs in the run table selection will be considered
# sel_dcstable="{groupName} ='"+GROUP+"' and {runNumber} >= "+RUNMIN+" and {runNumber} <= "+RUNMAX+" and {datasetName} LIKE '%Express%'"

dsfordcs="'%Prompt%'"
if ONLINE: dsfordcs="'%Global%Online%ALL'" 
sel_dcstable="{groupName} ='"+GROUP+"' and {runNumber} >= "+RUNMIN+" and {runNumber} <= "+RUNMAX+" and {datasetName} LIKE "+dsfordcs

for dcs in DCSLIST:
    if dcs !="NONE":
        sel_dcstable+=" and ( ( {cmpDcs"+DCS_ALL[dcs]+"} is null and {parDcs"+dcs+"} = 1 ) or {cmpDcs"+DCS_ALL[dcs]+"} = 1 )"
# = 'True'"
#print 'sel_dcstable: %s'%( sel_dcstable )

print " "
run_data=""
ls_temp_data=""

#socket.setdefaulttimeout(30)        #set the timeout to 120 seconds

try:
    print "Accessing run registry...."
    print " "
    dcs_data = server.DataExporter.export('RUNLUMISECTION', 'GLOBAL', 'json', sel_dcstable)
    for DSindex in range(0,len(DS_LIST)):
        run_data += server.DataExporter.export('RUN', 'GLOBAL', 'csv_runs', sel_runtable[DSindex])
        ls_temp_data += server.DataExporter.export('RUN', 'GLOBAL', 'csv_datasets', sel_dstable[DSindex])
        
except Exception, err:
    sys.stderr.write('ERROR: %s\n' % str(err))
    print "Run registry unaccessible.....exiting now"
    sys.exit(1)

#socket.setdefaulttimeout(None)        #set the timeout back to default

#print 'dcs_data: %s'%( dcs_data )
#print 'run_data: %s'%( run_data )
#print 'ls_temp_data: %s'%( ls_temp_data )
# find LS info in comment
#print run_data
#sys.exit(1)

LISTOFRUN=[]
for line in run_data.split("\n"):
    run=line.split(',')[0]
    if run.isdigit():
        LISTOFRUN.append(run)


#build up energy per run dictionary if asked to do so, and remove runs from lits
#print LISTOFRUN
if BEAMENE!="-1":
    print "Building up beam energy per run info... please wait it can be long"
#    energies=ene_in_run_old(LISTOFRUN)
    energies=ene_in_run(run_data)
    for run in energies.keys():
        if abs(energies[run]-float(BEAMENE))>400.0:
            LISTOFRUN.remove(run)
#print LISTOFRUN

# now check for DBS
if USEDBS:
    print "Accessing DBS to get lumi mapping: this can be VERY long...."
    dbsjson=get_dbsjson()
#    print dbsjson

selected_dcs={}
jsonlist=json.loads(dcs_data)


for element in jsonlist:
    if element in LISTOFRUN:
# first search manual ls certification
# this part really performs the run selection
        if LSCOMMENT:
            # using LS intervals in comment
            manualbad_int=searchrun(element)
        # make a badlumi list
            dcsbad_int=invert_intervals(jsonlist[element])
            combined=[]
            for interval in  manualbad_int:
                combined.append(interval)
            for interval in  dcsbad_int:
                combined.append(interval)

            # use this part in case DBS corss-check required
            if USEDBS:
                if element in dbsjson.keys():
                    dbsbad_int=invert_intervals(dbsjson[element])
                else:
                    # strange it should be there.... exclude all lumi for this run then...
                    dbsbad_int=[[1,9999]]
                for interval in  dbsbad_int:
                    combined.append(interval)

            combined=merge_intervals(combined)
            combined=invert_intervals(combined)
            if len(combined)!=0:
                selected_dcs[element]=combined
        else:
            # using only DCS info
            if len(jsonlist[element])!=0:
                selected_dcs[element]=jsonlist[element]
        # combined include bith manual LS and DCS LS



# WARNING: Don't use selected_dcs before dumping into file, it gets screwed up (don't know why!!)
if JSONFILE != "NONE":
    lumiSummary = open(JSONFILE, 'w')
    json.dump(selected_dcs, lumiSummary,sort_keys=True)
    lumiSummary.close()
    print " "
    print "-------------------------------------------"
    print "Json file: ",JSONFILE," written."


# buildup cms snippet
selectlumi="process.source.lumisToProcess = cms.untracked.VLuminosityBlockRange(\n"
ranges = []
runs_to_print = selected_dcs.keys()
runs_to_print.sort()
for run in runs_to_print:
   blocks = selected_dcs[run]
   blocks.sort()
   prevblock = [-2,-2]
   for lsrange in blocks:
       if lsrange[0] == prevblock[1]+1:
           print "Run: ",run,"- This lumi starts at ", lsrange[0], " previous ended at ", prevblock[1]+1, " so I should merge"
           prevblock[1] = lsrange[1]
           ranges[-1] = "\t'%s:%d-%s:%d',\n" % (run, prevblock[0],
run, prevblock[1])
       else:
           ranges.append("\t'%s:%d-%s:%d',\n" % (run, lsrange[0],
run, lsrange[1]))
           prevblock = lsrange
selectlumi += "".join(ranges)
selectlumi += ")"


print "-------------------------------------------"
print " "
print "CFG snippet to select:"
print selectlumi

if EXCEPTION:
    print "WARNING: Something wrong in manual lumisection selection tag for run: "+str(EXRUN)

# copy file with run range
#RUNINI=runs_to_print[0]
#RUNEND=runs_to_print[len(runs_to_print)-1]
#RUNTAG=RUNINI.strip()+"-"+RUNEND.strip()
#os.system("cp "+JSONFILE+" "+JSONFILE+"_"+RUNTAG)
