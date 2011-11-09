#!/usr/bin/env python

import sys,ConfigParser,os,string,commands,time,xmlrpclib
from operator import itemgetter

from optparse import OptionParser
from array import array
import json, csv
import analoss

SCENS=[
    ('DQM: all, DCS: all on','default','default'), 
    ('DQM: none, DCS: all on','NONE:NONE','default'),
    ('DQM: none, DCS: Strip on','NONE:NONE','Tibtid,TecM,TecP,Tob'),
    ('DQM: none, DCS: Pix on','NONE:NONE','Bpix,Fpix'),
    ('DQM: none, DCS: Ecal on','NONE:NONE','Ebminus,Ebplus,EeMinus,EePlus'),
    ('DQM: none, DCS: Es on','NONE:NONE','EsMinus,EsPlus'),
    ('DQM: none, DCS: HCAL on','NONE:NONE','HbheA,HbheB,HbheC,Hf'),
    ('DQM: none, DCS: Dt on','NONE:NONE','Dtminus,Dtplus,Dt0'),
    ('DQM: none, DCS: Csc on','NONE:NONE','CscMinus,CscPlus'),
    ('DQM: none, DCS: Rpc on','NONE:NONE','Rpc'),
    ('DQM: Muon, DCS: none','Muon:GOOD','NONE'),
    ('DQM: Jmet, DCS: none','Jmet:GOOD','NONE'),
    ('DQM: Egam, DCS: none','Egam:GOOD','NONE'),
    ('DQM: Track, DCS: none','Track:GOOD','NONE'),
    ('DQM: Rpc, DCS: Rpc','Rpc:GOOD','Rpc'),
    ('DQM: Csc, DCS: Csc','Csc:GOOD','CscMinus,CscPlus'),
    ('DQM: Dt, DCS: Dt','Dt:GOOD','Dtminus,Dtplus,Dt0'),
    ('DQM: Hcal, DCS: Hcal','Hcal:GOOD','HbheA,HbheB,HbheC,Hf'),
    ('DQM: Ecal, DCS: Ecal','Ecal:GOOD','Ebminus,Ebplus,EeMinus,EePlus'),
    ('DQM: Es, DCS: Es','Es:GOOD','EsMinus,EsPlus'),
    ('DQM: Strip, DCS: Strip','Strip:GOOD','Tibtid,TecM,TecP,Tob'),
    ('DQM: Pix, DCS: Pix','Pix:GOOD','Bpix,Fpix'),
    ('DQM: Hlt, DCS: none','Hlt:GOOD','NONE'),
    ('DQM: L1t, DCS: none','L1t:GOOD','NONE'),
    ('DQM: Lumi, DCS: none','Lumi:GOOD','NONE'),
    ('DQM: e+trk, DCS: e+trk','L1t:GOOD,Hlt:GOOD,Pix:GOOD,Strip:GOOD,Ecal:GOOD,Es:GOOD,Track:GOOD,Egam:GOOD,Lumi:GOOD','Bpix,Fpix,Tibtid,TecM,TecP,Tob,Ebminus,Ebplus,EeMinus,EePlus,EsMinus,EsPlus'),
    ('DQM: cal+trk, DCS: cal+trk','L1t:GOOD,Hlt:GOOD,Pix:GOOD,Strip:GOOD,Ecal:GOOD,Hcal:GOOD,Es:GOOD,Track:GOOD,Egam:GOOD,Jmet:GOOD,Lumi:GOOD','Bpix,Fpix,Tibtid,TecM,TecP,Tob,Ebminus,Ebplus,EeMinus,EePlus,EsMinus,EsPlus,HbheA,HbheB,HbheC,Hf'),
    ('DQM: muon phys, DCS: muon phys','L1t:GOOD,Hlt:GOOD,Pix:GOOD,Strip:GOOD,Dt:GOOD,Rpc:GOOD,Csc:GOOD,Track:GOOD,Muon:GOOD,Lumi:GOOD','Bpix,Fpix,Tibtid,TecM,TecP,Tob,Dtminus,Dtplus,Dt0,CscMinus,CscPlus,Rpc'),
    #####new scenario

    
    ('DQM: 9 subsystem ON but NOT Hlt, DCS: default','L1t:GOOD,Pix:GOOD,Strip:GOOD,Ecal:GOOD,Hcal:GOOD,Dt:GOOD,Rpc:GOOD,Es:GOOD,Csc:GOOD','default'),

    ('DQM: 9 subsystem ON but NOT L1t, DCS: default','Hlt:GOOD,Pix:GOOD,Strip:GOOD,Ecal:GOOD,Hcal:GOOD,Dt:GOOD,Rpc:GOOD,Es:GOOD,Csc:GOOD','default'),
    
    ('DQM: 9 subsystem ON but NOT Pix, DCS: default','L1t:GOOD,Hlt:GOOD,Strip:GOOD,Ecal:GOOD,Hcal:GOOD,Dt:GOOD,Rpc:GOOD,Es:GOOD,Csc:GOOD','default'),
    
    ('DQM: 9 subsystem ON but NOT Strip, DCS: default','L1t:GOOD,Hlt:GOOD,Pix:GOOD,Ecal:GOOD,Hcal:GOOD,Dt:GOOD,Rpc:GOOD,Es:GOOD,Csc:GOOD','default'),
    
    ('DQM: 9 subsystem ON but NOT Ecal, DCS: default','L1t:GOOD,Hlt:GOOD,Pix:GOOD,Strip:GOOD,Hcal:GOOD,Dt:GOOD,Rpc:GOOD,Es:GOOD,Csc:GOOD','default'),

    ('DQM: 9 subsystem ON but NOT Es, DCS: default','L1t:GOOD,Hlt:GOOD,Pix:GOOD,Strip:GOOD,Hcal:GOOD,Dt:GOOD,Rpc:GOOD,Ecal:GOOD,Csc:GOOD','default'),
    
    ('DQM: 9 subsystem ON but NOT Hcal, DCS: default','L1t:GOOD,Hlt:GOOD,Pix:GOOD,Strip:GOOD,Ecal:GOOD,Dt:GOOD,Rpc:GOOD,Es:GOOD,Csc:GOOD','default'),
    
    ('DQM: 9 subsystem ON but NOT Dt, DCS: default','L1t:GOOD,Hlt:GOOD,Pix:GOOD,Strip:GOOD,Ecal:GOOD,Hcal:GOOD,Rpc:GOOD,Es:GOOD,Csc:GOOD','default'),
    
    ('DQM: 9 subsystem ON but NOT CSC, DCS: default','L1t:GOOD,Hlt:GOOD,Pix:GOOD,Strip:GOOD,Ecal:GOOD,Hcal:GOOD,Dt:GOOD,Rpc:GOOD,Es:GOOD','default'),
    
    ('DQM: 9 subsystem ON but NOT RPC, DCS: default','L1t:GOOD,Hlt:GOOD,Pix:GOOD,Strip:GOOD,Ecal:GOOD,Hcal:GOOD,Dt:GOOD,Es:GOOD,Csc:GOOD','default')
    ]


# define relative losses as calculated from SCENS above
# example [3,0] means that losses of scenario 3 are compared (subracted) from losses in scenario 0
REL_LOSS=[[28,0],[29,0],[30,0],[31,0],[32,0],[33,0],[34,0],[35,0], [36,0], [37,0]]


REL_LOSS_VAL=[]

#parameters
QF_ALL_SYS=["Muon","Jmet","Egam","Track","Rpc","Csc","Dt","Hcal","Ecal","Es","Strip","Pix","Hlt","L1t","Lumi"]
DCS_ALL=['Bpix','Fpix','Tibtid','TecM','TecP','Tob','Ebminus','Ebplus','EeMinus','EePlus','EsMinus','EsPlus','HbheA','HbheB','HbheC','Hf','Dtminus','Dtplus','Dt0','CscMinus','CscPlus','Rpc','Castor',"NONE"]
#QF_ALL_SYS=["Hcal","Track","Strip","Egam","Es","Dt","Csc","Pix","Muon","Rpc","Castor","Jmet","Ecal","L1t","Hlt"]

# physics declared bit (~ stable beam) DCS equivalent
#DCS_PDB='Bpix,Fpix,Tibtid,TecM,TecP,Tob'

HTMLDIR=""
LUMICACHE=""
CERT_DIR=""

RUN_DATA=''
DEF_CFG=''
LUMI_CSV=''
LUMI_RESET=False
GLOB=False
KEEPRUNRANGE=False
NEW_DATASET_ALL=""

DEF_QF=''
DEF_DCS=''
RUN_TIME={}
RUN_TIME_00={}

CONFIG = ConfigParser.ConfigParser()

REPORT={}
JSONLIST=[]
TAG_NAME=[]

TOTLUMIDELIV={}
TOTLUMI={}
TOTLUMIACC={}

TOTLUMIDELIV_LW={}
TOTLUMI_LW={}
TOTLUMIACC_LW={}

TOTLUMIDELIV_NR={}
TOTLUMI_NR={}
TOTLUMIACC_NR={}


lumi_deliv={}
lumi_deliv_ov={}
lumi={}


RUNMIN=0
RUNMAX=0

RUNMINCFG=0
RUNNEWCFG=0
RUNMAXCFG=0

plot_type=[]
plot_xtype=[]
plot_xrange=[]

ultimate_date=''

def produce_json(SC,QF,DCS):
    global KEEPRUNRANGE,NEW_DATASET_ALL
    global RUNMINCFG,RUNNEWCFG,RUNMAXCFG

    JSON_ADD=str(SC)
    JSON_OUT="json_"+JSON_ADD+".txt"
    CONFIG.set("Common","jsonfile",JSON_OUT)
    CONFIG.set("Common","qflags", QF)
    CONFIG.set("Common","dcs",DCS)
    if not KEEPRUNRANGE:
        CONFIG.set("Common","runmax","999999")

    RUNMINCFG=CONFIG.get("Common","runmin")
    RUNNEWCFG=CONFIG.get("Common","runnew")
    RUNMAXCFG=CONFIG.get("Common","runmax")

    DATASET_ALL=CONFIG.get("Common","dataset")
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
            DS_RUNMIN.append(RUNMINCFG)
            DS_RUNMAX.append(RUNMAXCFG)
        else:
            print "Dataset in cfg not understood"
            sys.exit(1)

    NEW_DATASET_ALL=DATASET_ALL

    if not KEEPRUNRANGE:
        NEW_DATASET_ALL=""
        for DSindex in range(0,len(DS_LIST)):
            if DSindex==len(DS_LIST)-1:
                DS_RUNMAX[DSindex]="999999"
            NEW_DATASET_ALL+=DS_LIST[DSindex]+":"+DS_RUNMIN[DSindex]+"-"+DS_RUNMAX[DSindex]+" "


    CONFIG.set("Common","dataset",NEW_DATASET_ALL)

    CURR_CFG = "json_" + JSON_ADD + ".cfg"
    CURR_CFG_FP=open(CURR_CFG,'wb')
    CONFIG.write(CURR_CFG_FP)


    CURR_CFG_FP.close()
    LOGFILE="json_"+JSON_ADD+".log"
    COMMAND="./runregparse.py "+CURR_CFG+" > "+LOGFILE
    print "======================================================================================"
    print "Producing scenario:",SC
    print "            QFLAGS:",QF
    print "               DCS:",DCS
    print "              JSON:",JSON_OUT
    print "           command:",COMMAND
    print "======================================================================================"
    list=[QF,DCS,JSON_OUT]
    REPORT[SC]=list
    os.system(COMMAND)
    return JSON_OUT

################################################################################
def readlumi():
    global lumi,LUMI_CSV
    global TAG_NAME

    lumi_list_csv = open(LUMI_CSV,'rb')
    # Skip first 5 lines, which are comments
    for i in range(5):
    	lumi_list_csv.next()

    lumi_dict = csv.DictReader(lumi_list_csv,delimiter=',',fieldnames=['Run','LS','HFC','VTXC','HFL','VTXL'])

    for l in lumi_dict:
    	kw="%d_%d" % (int(l['Run']),int(l['LS']))
    	lumi[kw]=float(l['VTXL'])

    lumi_list_csv.close()

def readlumi_db():
    global lumi,lumi_deliv,lumi_deliv_ov,LUMI_RESET,LUMI_CACHE
    global RUN_TIME

    runlist=RUN_TIME.keys()
    runlist.sort()

    runmaxincache=-1
    if not LUMI_RESET:
        if os.path.exists(LUMICACHE):
            
            print "opening lumicache"
            f=open(LUMICACHE,'r')

            for line in f:
                
                column=line.split()
                key=column[0]
                if len(column)==4:
                    lumi_deliv[key]=float(column[1])
                    lumi[key]=float(column[2])
                    lumi_deliv_ov[key]=float(column[3])
                    runno=key.split('_')[0]
                    
                    if int(runno)>runmaxincache:
                        if runno.strip() in runlist:
                            runmaxincache=int(runno)
            f.close()
        else:
            print "Lumicache file not existing....recreating it from scratch"
            LUMI_RESET=True

    # RUN_TIME contains only the selected collisions10 runs (it may includes 900GeV data.... should be checked)
    print "Maximum run number in local cache: ",runmaxincache



    curr_time=time.time()
    
    print runlist
    
    for runno in runlist:
        if runno.isdigit():
            # check if the runno is inside 2 days of last run in cache
            if not LUMI_RESET and runmaxincache!=-1:
                if RUN_TIME[runno]<(RUN_TIME[str(runmaxincache)]-86400*2):
                    continue

            # otherwise make the query
            try:
                print "Accessing LumiDB for run: "+runno
                # if using stable beam mode, please consider that the flag is only set for run>=132716
                # the lumi between run 132440 and 132716 is totally negligible: 0.1 nb-1
#                lumitable=commands.getoutput("lumiCalc.py  -c frontier://LumiProd/CMS_LUMI_PROD lumibyls -r "+runno)
                lumitable=commands.getoutput("lumiCalc2.py  -c frontier://LumiProd/CMS_LUMI_PROD lumibyls -r "+runno+" -b stable")
                lumitable_overview=commands.getoutput("lumiCalc2.py  -c frontier://LumiProd/CMS_LUMI_PROD overview -r "+runno+" -b stable")
            except:
                print "Problem in accessing lumidb for run:"+runno

            tmplumi_deliv=0.
            for line in lumitable_overview.split("\n"):
            
                column=line.split('|')
                
                
                if len(column)==7:
                    if column[1].strip().isdigit():
                        if int(column[1])==int(runno):
                            try:
                                
                                if '(/pb)' in column[3]:
                                    str_ls=column[3].split('(/pb)')
                                if '(/nb)' in column[3]:
                                    str_ls=column[3].split('(/nb)')
                                    str_ls=str_ls/1e03
                                if '(/ub)' in column[3]:
                                    str_ls=column[3].split('(/ub)')
                                    str_ls=str_ls/1e06
                                
                                tmplumi_deliv=float(str_ls[0])*1e06
                                
                            except:
                                tmplumi_deliv=0.
                                
           
            firstls=True
#            nostablebeam=True
            for line in lumitable.split("\n"):
                column=line.split('|')
                if len(column)==9:
                  
                    if column[1].strip().isdigit():
                        if int(column[1])==int(runno):
                            nostablebeam=False
                            str_ls=column[2].split(':')
                            kw="%d_%d" % (int(column[1]),int(str_ls[0]))
                           
                            lumi[kw]=float(column[7])
                                                       
                            lumi_deliv[kw]=float(column[6])
                            
                            if firstls:
                              
                                lumi_deliv_ov[kw]=tmplumi_deliv
                               
                                firstls=False
                               
                            else:
                                lumi_deliv_ov[kw]=0.
                        
#            if nostablebeam:
#                kw="%d_%d" % (int(runno),-1)
 
                
    # now need to write new lumicache
    keylist=lumi.keys()
    keylist.sort()
    f=open(LUMICACHE,'w')
    for key in keylist:
        f.write(key+" "+str(lumi_deliv[key])+" "+str(lumi[key])+" "+str(lumi_deliv_ov[key])+"\n")
    f.close()


def makeplot(cur):
    from ROOT import TCanvas,TGraph,gStyle,gROOT,TH1F,TLatex
    global TOTLUMI,TOTLUMIACC,TOTLUMI_LW,TOTLUMIACC_LW,TOTLUMI_NR,TOTLUMIACC_NR
    global TOTLUMIDELIV,TOTLUMIDELIV_LW,TOTLUMIDELIV_NR
    global RUNMIN,RUNMAX
    global LUMI_CSV
    global lumi,lumi_deliv,lumi_deliv_ov
    global plot_type,plot_xtype,plot_xrange
    global ultimate_date

    kTRUE=True

    plot_type=['lumi','loss']
    plot_xtype=['run','time']
    plot_xrange=['tot','lastweek','new']


    print " "
    print "Producing plot for scenario:",cur
    print TAG_NAME[cur]

    # use this for batch mode
    gROOT.SetBatch(True)

    repref=REPORT[len(JSONLIST)-1] # always use PBD as reference
    repcur=REPORT[cur]

    lumi_json_ref=file(repref[2],'r')
    json_ref=json.load(lumi_json_ref)

    my_lumi_json=file(repcur[2],'r')
    my_lumi_dict = json.load(my_lumi_json)

    # determine runmin and runmax
    RUNMIN_JSON=999999
    RUNMAX_JSON=0
    for run in my_lumi_dict.keys():
    	if int(run)>RUNMAX_JSON:
    		RUNMAX_JSON=int(run)
    	if int(run)<RUNMIN_JSON:
    		RUNMIN_JSON=int(run)
    print "Run Range in JSON file:",RUNMIN_JSON,"-",RUNMAX_JSON

    # determine runmin and runmax in RR
    RUNMIN=999999
    RUNMAX=0

    runlist_rr=RUN_TIME.keys()
    runlist_rr.sort()
    RUNMIN=int(runlist_rr[0])
    RUNMAX=int(runlist_rr[len(runlist_rr)-1])
    print "Run Range in Run Registry:",RUNMIN,"-",RUNMAX


    RUNMIN_LUMI=999999
    RUNMAX_LUMI=0

    for l in lumi:
        str_run=l.split('_')[0]
        str_run=str_run.strip()
        str_ls=l.split('_')[1]
        str_ls=str_ls.strip()
        curr_run=int(str_run)
        if curr_run<RUNMIN_LUMI:
            RUNMIN_LUMI=curr_run
        if curr_run>RUNMAX_LUMI:
            RUNMAX_LUMI=curr_run
    print "Run Range in LUMI file:",RUNMIN_LUMI,"-",RUNMAX_LUMI

    if RUNMIN_LUMI>RUNMIN:
        print "WARNING!: min run number in lumi file is BIGGER than min run number in RR"
        print "WARNING!: setting RUNMIN to the one from lumi file:",RUNMIN_LUMI
        RUNMIN=RUNMIN_LUMI
    if RUNMAX_LUMI<RUNMAX:
        print "WARNING!: max run number in lumi file is SMALLER than max run number in RR"
        print "WARNING!: setting RUNMAX to the one from lumi file:",RUNMAX_LUMI
        RUNMAX=RUNMAX_LUMI




    delivlumiinrun={}
    reclumiinrun={}
    acclumiinrun={}

    tot_rec=1e-30
    tot_rec_lw=1e-30
    tot_rec_nr=1e-30
    tot_deliv=1e-30
    tot_deliv_lw=1e-30
    tot_deliv_nr=1e-30

    # determine run boundary for "last week" run
    lastrun_time=RUN_TIME[str(RUNMAX)]


    coeff=4.29e28
    if LUMI_CSV=='none':
        coeff=1.

    firstrun_lw=999999
    firstrun_nr=999999



    # builds up recorded lumi in base runlist (i.e. no good run selection but just runs with stable beam)
   
    for l in lumi:
        str_run=l.split('_')[0]
        str_run=str_run.strip()
        str_ls=l.split('_')[1]
        str_ls=str_ls.strip()
        curr_run=int(str_run)
        
        # checks as well that the run is in the reference json to exclude bad runs (wrong energy, no collisions, etc.)
#    	if int(str_run)>=RUNMIN and int(str_run)<=RUNMAX and str_run in json_ref.keys():
    	if int(str_run)>=RUNMIN and int(str_run)<=RUNMAX and str_run in RUN_TIME.keys():
    		tot_rec+=lumi[l]
#    		tot_deliv+=lumi_deliv[l]

    		tot_deliv+=lumi_deliv_ov[l]
              
                if RUN_TIME[str_run]>(lastrun_time-86400*7):
                    tot_rec_lw+=lumi[l]
#                    tot_deliv_lw+=lumi_deliv[l]
                  
                    tot_deliv_lw+=lumi_deliv_ov[l]
                    
                    
                    if int(str_run)<firstrun_lw:
                        firstrun_lw=int(str_run)
                if int(str_run)>=int(RUNNEWCFG):
                    tot_rec_nr+=lumi[l]
#                    tot_deliv_nr+=lumi_deliv[l]
                    tot_deliv_nr+=lumi_deliv_ov[l]
                    if int(str_run)<firstrun_nr:
                        firstrun_nr=int(str_run)

    		if str_run in reclumiinrun.keys():
    			reclumiinrun[str_run]+=lumi[l]/coeff/1e+6
#    			delivlumiinrun[str_run]+=lumi_deliv[l]/coeff/1e+6
    			delivlumiinrun[str_run]+=lumi_deliv_ov[l]/coeff/1e+6
    		else:
    			reclumiinrun[str_run]=lumi[l]/coeff/1e+6
#    			delivlumiinrun[str_run]=lumi_deliv[l]/coeff/1e+6
    			delivlumiinrun[str_run]=lumi_deliv_ov[l]/coeff/1e+6

    tot=1e-30
    tot_lw=1e-30
    tot_nr=1e-30

    # builds up accepted lumi in good runs
    for k, v in my_lumi_dict.items():
      for lumis in v:
        if type(lumis) == type([]) and len(lumis) == 2:
          for i in range(lumis[0], lumis[1] + 1):
    		kw="%d_%d" % (int(k),int(i))
    		if kw in lumi:
    			tot+=lumi[kw]
    			str_run=str(k).strip()
                        if RUN_TIME[str_run]>(lastrun_time-86400*7):
                            tot_lw+=lumi[kw]
                        if int(str_run)>=int(RUNNEWCFG):
                            tot_nr+=lumi[kw]
    			if str_run in acclumiinrun.keys():
    				acclumiinrun[str_run]+=lumi[kw]/coeff/1e+6
    			else:
    				acclumiinrun[str_run]=lumi[kw]/coeff/1e+6

    ll = tot / coeff
    ll_rec = tot_rec/coeff
    ll_deliv = tot_deliv/coeff

    ll_lw = tot_lw / coeff
    ll_rec_lw = tot_rec_lw/coeff
    ll_deliv_lw = tot_deliv_lw/coeff

    ll_nr = tot_nr / coeff
    ll_rec_nr = tot_rec_nr/coeff
    ll_deliv_nr = tot_deliv_nr/coeff

    print "In the run range:",RUNMIN,"-",RUNMAX
    print "Delivered luminosity: %2.2f /pb" % (ll_deliv/1e+6)
    print "Recorded  luminosity: %2.2f /pb" % (ll_rec/1e+6)
    print "Certified luminosity: %2.2f /pb" % (ll/1e+6)
    print "Rejected  luminosity: %2.2f /pb" % ((ll_rec-ll)/1e+6)
    print "Efficiency of certified luminosity: %2.2f%%" % (100.*ll/ll_rec)
    print "Delivered luminosity last week: %2.2f /pb" % (ll_deliv_lw/1e+6)
    print "Recorded  luminosity last week: %2.2f /pb" % (ll_rec_lw/1e+6)
    print "Certified luminosity last week: %2.2f /pb" % (ll_lw/1e+6)
    print "Rejected  luminosity last week: %2.2f /pb" % ((ll_rec_lw-ll_lw)/1e+6)
    print "Efficiency of certified luminosity last week: %2.2f%%" % (100.*ll_lw/ll_rec_lw)
    print "Delivered luminosity new runs: %2.2f /pb" % (ll_deliv_nr/1e+6)
    print "Recorded  luminosity new runs: %2.2f /pb" % (ll_rec_nr/1e+6)
    print "Certified luminosity new runs: %2.2f /pb" % (ll_nr/1e+6)
    print "Rejected  luminosity new runs: %2.2f /pb" % ((ll_rec_nr-ll_nr)/1e+6)
    print "Efficiency of certified luminosity new runs: %2.2f%%" % (100.*ll_nr/ll_rec_nr)

    TOTLUMIACC[cur]=ll
    TOTLUMI[cur]=ll_rec
    TOTLUMIDELIV[cur]=ll_deliv
    TOTLUMIACC_LW[cur]=ll_lw
    TOTLUMI_LW[cur]=ll_rec_lw
    TOTLUMIDELIV_LW[cur]=ll_deliv_lw
    TOTLUMIACC_NR[cur]=ll_nr
    TOTLUMI_NR[cur]=ll_rec_nr
    TOTLUMIDELIV_NR[cur]=ll_deliv_nr

    c_rec={}
    c_acc={}
    c_deliv={}
    c_rec_diff={}
    c_acc_diff={}
    c_deliv_diff={}


    c_rec_curr=0.
    c_acc_curr=0.
    c_deliv_curr=0.

    listofrun=[]
    for run in reclumiinrun.keys():
    	listofrun.append(int(run))
    listofrun.sort()

    for runno in listofrun:
    	run=str(runno)
    	c_rec_curr+=reclumiinrun[run]
    	c_rec[run]=c_rec_curr
    	c_rec_diff[run]=reclumiinrun[run]

    	c_deliv_curr+=delivlumiinrun[run]
    	c_deliv[run]=c_deliv_curr
    	c_deliv_diff[run]=delivlumiinrun[run]

    	if run in acclumiinrun.keys():
            c_acc_curr+=acclumiinrun[run]
            c_acc_diff[run]=acclumiinrun[run]
        else:
            c_acc_diff[run]=0.

    	c_acc[run]=c_acc_curr

    # preparing now the graphs

    for p_type in plot_type:
        for p_xtype in plot_xtype:
            for p_xrange in plot_xrange:
                x=array('d')
                y1=array('d') # first graph on plot
                y2=array('d') # second graph on plot
                y3=array('d') # second graph on plot
                label=[]
                numpoint=0
                ymax=0.0001
                ymin=0.0
                b_y1=0.0
                b_y2=0.0
                b_y3=0.0



                rec_lumi=0.
                acc_lumi=0.
                deliv_lumi=0.
                for runno in listofrun:
                    last_rec_lumi=rec_lumi
                    last_acc_lumi=acc_lumi
                    last_deliv_lumi=deliv_lumi
                    rec_lumi=c_rec[str(runno)]
                    acc_lumi=c_acc[str(runno)]
                    deliv_lumi=c_deliv[str(runno)]

                    if "lastweek" in p_xrange:
                        if runno<firstrun_lw:
                            continue
                    if "new" in p_xrange:
                        if runno<firstrun_nr:
                            continue

                    if  'run' in p_xtype:
                        x.append(numpoint+0.5)
                        label.append(str(runno))
                    elif 'time' in p_xtype:
                        x.append(RUN_TIME[str(runno)])

                    if 'lumi' in p_type:
                        y1.append(rec_lumi)
                        y2.append(acc_lumi)
                        y3.append(deliv_lumi)
                        if "lastweek" in p_xrange or "new" in p_xrange:
                            if len(y1)==1:
                                b_y1=last_rec_lumi
                                b_y2=last_acc_lumi
                                b_y3=last_deliv_lumi
                            y1[len(y1)-1]-=b_y1
                            y2[len(y1)-1]-=b_y2
                            y3[len(y1)-1]-=b_y3

                        if deliv_lumi>ymax:
                            ymax=y3[len(y1)-1]
                    elif 'loss' in p_type:
                        loss=c_acc_diff[str(runno)]-c_rec_diff[str(runno)]
                        y1.append(loss)
                        if loss < ymin:
                            ymin=loss

                    numpoint+=1

    # ready for the plots
    # then histos
    # run based histos

                name=''
                title=''
                xlabel=''
                ylabel=''
                nbin=0
                xlow=0
                xhigh=0
                if 'lumi' in p_type:
                    name="IntegratedLuminosity"
#                     title="Integrated luminosity"
                    ylabel="L_{int} (pb^{-1})"
                if 'loss' in p_type:
                    name="LossOfIntegratedLuminosity"
#                     title="Loss of integrated luminosity"
                    ylabel="L_{int} (pb^{-1})"
                if 'run' in p_xtype:
                    xlabel="Run"
                    nbin=numpoint
                    xlow=0
                    xhigh=numpoint
                if 'time' in p_xtype:
                    if "lastweek" in p_xrange:
                        time_day_min=RUN_TIME_00[str(firstrun_lw)]
                    elif "new" in p_xrange:
                        time_day_min=RUN_TIME_00[str(firstrun_nr)]
                    elif "tot" in p_xrange:
                        time_day_min=RUN_TIME_00[str(listofrun[0])]

                    time_day_max=RUN_TIME_00[str(listofrun[len(listofrun)-1])] # at midnight next day
                    numdays = int(time_day_max-time_day_min+2*86400)/86400

                    timehistomin=time_day_min
                    timehistomax=numdays*86400+timehistomin
                    xlabel="Date"
                    nbin=numdays
                    xlow=0
                    xhigh=numdays

                plot= TH1F(name,title+";"+xlabel+";"+ylabel,nbin,xlow,xhigh)

                labelx=''
                for ibin in range(0,nbin):
                    if 'run' in p_xtype:
                        labelx=label[ibin]
                    elif 'time' in p_xtype:
                        labelx=time.strftime("%d %b",time.localtime(timehistomin+(ibin*86400)))
                        if ibin is ( nbin - 1 ):
                            ultimate_date = labelx
                        #if not ( ibin == 0 or ibin == ( nbin - 1 ) or labelx[ : 2 ] == "01" ):
                        if "tot" in p_xrange and not ( labelx[ : 2 ] == "01" ):
                            labelx = ""
                    plot.GetXaxis().SetBinLabel(ibin+1,labelx)


    # then define graphs
    # first renormalise in case of time graph (otherwise root makes wrong divisions, no idea why)
                if 'time' in p_xtype:
                    for point in range(0,int(numpoint)):
                        x[point]=(x[point]-timehistomin)/86400.

                gr1=TGraph(int(numpoint),x,y1)
                gr1.SetMarkerColor(2)
                gr1.SetMarkerStyle(20)
                gr1.SetMarkerSize(0.8)
                if 'lumi' in p_type:
                    gr2=TGraph(int(numpoint),x,y2)
                    gr2.SetMarkerColor(4)
                    gr2.SetMarkerStyle(20)
                    gr2.SetMarkerSize(0.8)
                    gr3=TGraph(int(numpoint),x,y3)
                    gr3.SetMarkerColor(1)
                    gr3.SetMarkerStyle(20)
                    gr3.SetMarkerSize(0.8)
                    if 'run' in p_xtype:
                        gr1.SetTitle("Luminosity vs. run number")
                        gr2.SetTitle("Luminosity vs. run number")
                        gr3.SetTitle("Luminosity vs. run number")
                    elif 'time' in p_xtype:
                        gr1.SetTitle("Luminosity vs. time")
                        gr2.SetTitle("Luminosity vs. time")
                        gr3.SetTitle("Luminosity vs. time")
                elif 'loss' in p_type:
                    if 'run' in p_xtype:
                        gr1.SetTitle("Luminosity loss vs. run number")
                    elif 'time' in p_xtype:
                        gr1.SetTitle("Luminosity loss vs. time")





# now really finally plotting it....

                gStyle.SetOptStat(0)
                gStyle.SetOptFit(0)
                plot.LabelsOption("v","X")
                plot.SetMaximum(ymax*1.05)
                plot.SetMinimum(ymin*1.05)

                c1 = TCanvas("c1","c1",800,600)
                c1.SetFillColor(0)
                plot.SetFillColor(0)
                plot.Draw()

                gr1.Draw("PL")


                if "lumi" in p_type:
                    lumitext="Recorded: "
                    lumiacctext="Certified: "
                    lumidelivtext="Delivered: "
                    legendtext = ""
                    if "lastweek" in p_xrange:
                        legendtext   ="Last week"
                        lumitext     +="%2.2f pb^{-1}" % (TOTLUMI_LW[cur]/1e+6)
                        lumiacctext  +="%2.2f pb^{-1}" % (TOTLUMIACC_LW[cur]/1e+6)
                        lumidelivtext+="%2.2f pb^{-1}" % (TOTLUMIDELIV_LW[cur]/1e+6)
                    elif "new" in p_xrange:
                        legendtext   ="New runs"
                        lumitext     +="%2.2f pb-1" % (TOTLUMI_NR[cur]/1e+6)
                        lumiacctext  +="%2.2f pb-1" % (TOTLUMIACC_NR[cur]/1e+6)
                        lumidelivtext+="%2.2f pb-1" % (TOTLUMIDELIV_NR[cur]/1e+6)
                    elif "tot" in p_xrange:
                        legendtext   ="All runs"
                        lumitext     +="%2.2f pb^{-1}" % (TOTLUMI[cur]/1e+6)
                        lumiacctext  +="%2.2f pb^{-1}" % (TOTLUMIACC[cur]/1e+6)
                        lumidelivtext+="%2.2f pb^{-1}" % (TOTLUMIDELIV[cur]/1e+6)

                    xcoord = 0.15
                    ycoord = 0.82
                    tex0 = TLatex(xcoord,ycoord,"CMS preliminary 2011")
                    tex0.SetNDC(kTRUE)
                    tex0.SetTextSize(0.04)
                    tex0.SetTextColor(1)
                    tex0.SetLineWidth(2)
                    tex0.Draw()

                    ycoord -= 0.04
                    tex0a = TLatex(xcoord,ycoord,"Integrated luminosity")
                    tex0a.SetNDC(kTRUE)
                    tex0a.SetTextSize(0.05)
                    tex0a.SetTextColor(1)
                    tex0a.SetLineWidth(2)
                    tex0a.Draw()

                    if 'tot' in p_xrange:
                        legendtext +=" until %s 2011"%(ultimate_date)
                    ycoord -= 0.05
                    tex0b = TLatex(xcoord,ycoord,legendtext)
                    tex0b.SetNDC(kTRUE)
                    tex0b.SetTextSize(0.03)
                    tex0b.SetTextColor(1)
                    tex0b.SetLineWidth(2)
                    tex0b.Draw()

                    gr3.Draw("PL")
                    ycoord -= 0.05
                    tex2a = TLatex(xcoord,ycoord,lumidelivtext)
                    tex2a.SetNDC(kTRUE)
                    tex2a.SetTextSize(0.04)
                    tex2a.SetTextColor(1)
                    tex2a.SetLineWidth(1)
                    tex2a.Draw()

                    ycoord -= 0.04
                    tex = TLatex(xcoord,ycoord,lumitext)
                    tex.SetNDC(kTRUE)
                    tex.SetTextSize(0.04)
                    tex.SetTextColor(2)
                    tex.SetLineWidth(1)
                    tex.Draw()

                    gr2.Draw("PL")
                    ycoord -= 0.04
                    tex2 = TLatex(xcoord,ycoord,lumiacctext)
                    tex2.SetNDC(kTRUE)
                    tex2.SetTextSize(0.04)
                    tex2.SetTextColor(4)
                    tex2.SetLineWidth(1)
                    tex2.Draw()

                tex3 = TLatex(0.5,0.95,TAG_NAME[cur])
                tex3.SetNDC(kTRUE)
                tex3.SetTextSize(0.03)
                tex3.SetTextColor(4)
                tex3.SetLineWidth(2)
                tex3.Draw()


                c1.Update()
                pngfile='plot_'+p_type+'_'+p_xtype+'_'+p_xrange+'_'+str(cur)+'.png'
                cfile='plot_'+p_type+'_'+p_xtype+'_'+p_xrange+'_'+str(cur)+'.C'
                c1.Print(pngfile)
                c1.Print(cfile)

                del c1
                del plot
                del gr1
                if 'lumi' in p_type:
                    del gr2
                    del gr3

    return True



def makesummaryplot():
    from ROOT import TCanvas,TGraph,gStyle,gROOT,TH1F,TLatex
    global TOTLUMI,TOTLUMIACC,TOTLUMI_LW,TOTLUMIACC_LW,TOTLUMI_NR,TOTLUMIACC_NR
    global TAG_NAME

    kTRUE=True

    print " "
    print "Producing summary plot"

    # use this for interactive mode
#    gROOT.SetBatch(False)
    # use this for batch mode
    gROOT.SetBatch(True)


    # preparing now the graphs
    x=array('d')
    y=array('d')
    y_lw=array('d')
    y_nr=array('d')

    label=[]
    label_lw=[]
    label_nr=[]

    numpoint=0

    for scenario in range(0,len(JSONLIST)):
        x.append(numpoint)
        if  TOTLUMI[scenario] != 0:
            effi=TOTLUMIACC[scenario]/TOTLUMI[scenario]
        else:
            effi=0.

        y.append(effi)
    	label.append(TAG_NAME[scenario]+" -- %5.3f" %(effi))

        if  TOTLUMI_LW[scenario] != 0:
            effi=TOTLUMIACC_LW[scenario]/TOTLUMI_LW[scenario]
        else:
            effi=0.
        y_lw.append(effi)
    	label_lw.append(TAG_NAME[scenario]+" -- %5.3f" %(effi))

        if  TOTLUMI_NR[scenario] != 0:
            effi=TOTLUMIACC_NR[scenario]/TOTLUMI_NR[scenario]
        else:
            effi=0.
        y_nr.append(effi)
    	label_nr.append(TAG_NAME[scenario]+" -- %5.3f" %(effi))

        numpoint+=1


    chart_tot = TH1F("AcceptedEffi",";;",numpoint,0,numpoint)
    chart_lw = TH1F("AcceptedEffiLw",";;",numpoint,0,numpoint)
    chart_nr = TH1F("AcceptedEffiNr",";;",numpoint,0,numpoint)

    for ibin in range(0,numpoint):
        chart_tot.GetXaxis().SetBinLabel(ibin+1,label[ibin])
        chart_lw.GetXaxis().SetBinLabel(ibin+1,label_lw[ibin])
        chart_nr.GetXaxis().SetBinLabel(ibin+1,label_nr[ibin])

    gStyle.SetOptStat(0)

    chart_tot.SetLabelOffset(-0.2,"X")
    chart_lw.SetLabelOffset(-0.2,"X")
    chart_nr.SetLabelOffset(-0.2,"X")

    chart_tot.SetMaximum(1.001)
    chart_lw.SetMaximum(1.001)
    chart_nr.SetMaximum(1.001)
    chart_tot.SetFillColor(5)
    chart_lw.SetFillColor(5)
    chart_nr.SetFillColor(5)

    # we have to fill them
    for bar in range(0,numpoint):
        chart_tot.Fill(bar,y[bar])
        chart_lw.Fill(bar,y_lw[bar])
        chart_nr.Fill(bar,y_nr[bar])

    c1 = TCanvas("c1","c1",600,800)
    c1.SetFillColor(0)
    c1.SetTopMargin(0.19)
    c1.SetGridx()
    c1.SetTicky(0)

    chart_tot.Draw("hbar1")

    xcoord = 0.10
    ycoord = 0.92
    tex0 = TLatex(xcoord,ycoord,"CMS preliminary 2011")
    tex0.SetNDC(kTRUE)
    tex0.SetTextSize(0.03)
    tex0.SetTextColor(1)
    tex0.SetLineWidth(2)
    tex0.Draw()

    ycoord -= 0.04
    tex0a = TLatex(xcoord,ycoord,"Current certification efficiencies")
    tex0a.SetNDC(kTRUE)
    tex0a.SetTextSize(0.04)
    tex0a.SetTextColor(1)
    tex0a.SetLineWidth(2)
    tex0a.Draw()

    legendtext ="All runs until %s 2011"%(ultimate_date)
    ycoord -= 0.03
    tex1 = TLatex(xcoord,ycoord,legendtext)
    tex1.SetNDC(kTRUE)
    tex1.SetTextSize(0.03)
    tex1.SetTextColor(1)
    tex1.SetLineWidth(2)
    tex1.Draw()

    lumitext ="Recorded integrated luminosity: %2.2f pb^{-1}"%(TOTLUMI[0]/1e+6)
    ycoord -= 0.03
    tex1a = TLatex(xcoord,ycoord,lumitext)
    tex1a.SetNDC(kTRUE)
    tex1a.SetTextSize(0.03)
    tex1a.SetTextColor(1)
    tex1a.SetLineWidth(2)
    tex1a.Draw()

    c1.Update()
    c1.Print('plot_summary_tot'+'.png')
    c1.Print('plot_summary_tot'+'.C')

    c2 = TCanvas("c2","c2",600,800)
    c2.SetFillColor(0)
    c2.SetTopMargin(0.19)
    c2.SetGridx()
    c2.SetTicky(0)

    chart_lw.Draw("hbar1")

    tex0.Draw()
    tex0a.Draw()

    ycoord += 0.03
    tex2 = TLatex(xcoord,ycoord,"Last week")
    tex2.SetNDC(kTRUE)
    tex2.SetTextSize(0.03)
    tex2.SetTextColor(1)
    tex2.SetLineWidth(2)
    tex2.Draw()

    lumitext ="Recorded integrated luminosity: %2.2f pb^{-1}" % (TOTLUMI_LW[0]/1e+6)
    ycoord -= 0.03
    tex2a = TLatex(xcoord,ycoord,lumitext)
    tex2a.SetNDC(kTRUE)
    tex2a.SetTextSize(0.03)
    tex2a.SetTextColor(1)
    tex2a.SetLineWidth(2)
    tex2a.Draw()

    c2.Update()
    c2.Print('plot_summary_lw'+'.png')
    c2.Print('plot_summary_lw'+'.C')

    c3 = TCanvas("c3","c3",600,800)
    c3.SetFillColor(0)
    c3.SetTopMargin(0.19)
    c3.SetGridx()
    c3.SetTicky(0)

    chart_nr.Draw("hbar1")

    tex0.Draw()
    tex0a.Draw()

    ycoord += 0.03
    tex3 = TLatex(xcoord,ycoord,"New runs")
    tex3.SetNDC(kTRUE)
    tex3.SetTextSize(0.03)
    tex3.SetTextColor(1)
    tex3.SetLineWidth(2)
    tex3.Draw()

    lumitext ="Recorded integrated luminosity: %2.2f pb^{-1}" % (TOTLUMI_NR[0]/1000000.)
    ycoord -= 0.03
    tex3a = TLatex(xcoord,ycoord,lumitext)
    tex3a.SetNDC(kTRUE)
    tex3a.SetTextSize(0.03)
    tex3a.SetTextColor(1)
    tex3a.SetLineWidth(2)
    tex3a.Draw()

    c3.Update()
    c3.Print('plot_summary_nr'+'.png')
    c3.Print('plot_summary_nr'+'.C')

    return True




###################################################################################
def looponscenario():
    global TIME_DAYBOUND_MAX,TIME_DAYBOUND_MIN
    global JSONLIST
    global RUN_DATA,RUN_TIME
    global CERT_DIR
    global KEEPRUNRANGE
    global RUNMINCFG,RUNMAXCFG

    print 'Reading default configuration file from ',DEF_CFG
    CONFIG.read(DEF_CFG)
    # storing the default request
    DEF_QF=CONFIG.get('Common','QFLAGS')
    DEF_DCS=CONFIG.get('Common','DCS')

    DEF_QFS =string.split(DEF_QF, ',')
    DEF_DCSS=string.split(DEF_DCS, ',')


    # first remove the dbs option.... too expensive
    CONFIG.remove_option("Common","dbs_pds")

    # really loop on scenarios
    for scen in range(0,len(SCENS)):
#    for scen in range(0,2):
        (curr_tagname,curr_qf,curr_dcs)=SCENS[scen]
        if curr_qf=='default': curr_qf=DEF_QF
        if curr_dcs=='default': curr_dcs=DEF_DCS
        JSONLIST.append(produce_json(len(JSONLIST),curr_qf,curr_dcs))
        TAG_NAME.append(curr_tagname)


#    # first produce the full with all required DCS on
#    JSONLIST.append(produce_json(len(JSONLIST),DEF_QF,DEF_DCS))
#    TAG_NAME.append("DQM: all, DCS: all on")
#
#    # then only all DCS on
#    JSONLIST.append(produce_json(len(JSONLIST),"NONE:NONE",DEF_DCS))
#    TAG_NAME.append("DQM: none, DCS: all on")
#
#    # then the full with only requirement tracker on
#    JSONLIST.append(produce_json(len(JSONLIST),DEF_QF,DCS_PDB))
#    TAG_NAME.append("DQM: all, DCS: Tracker on")
#    # then per sub-system with only requirement tracker on
#    for qf in range(len(QF_ALL_SYS)):
#        JSONLIST.append(produce_json(len(JSONLIST),QF_ALL_SYS[qf]+":GOOD",DCS_PDB))
#        TAG_NAME.append("DQM: "+QF_ALL_SYS[qf]+", DCS: Tracker on")
#    # finally only tracker on
#    JSONLIST.append(produce_json(len(JSONLIST),"NONE:NONE",DCS_PDB))
#    TAG_NAME.append("DQM: none, DCS: Tracker on")


    # access run registry to get the full list of runs and start/end time
   
    server = xmlrpclib.ServerProxy("http://cms-service-runregistry-api.web.cern.ch/cms-service-runregistry-api/xmlrpc")
    #server = xmlrpclib.ServerProxy("http://pccmsdqm04.cern.ch/runregistry_api/xmlrpc")
    #sel_runtable="{groupName} = 'Collisions10' and {datasetName} LIKE '%Express%' and {bfield}>3.7 and {runNumber} >= "+RUNMINCFG+" and {runNumber} <= "+RUNMAXCFG 
    sel_runtable="{groupName} = 'Collisions11' and {datasetName} LIKE '%Prompt%'  and {bfield}>3.7 and {runNumber} >= "+RUNMINCFG+" and {runNumber} <= "+RUNMAXCFG

    # NEW CKR 10.10.2011:
    #sel_runtable="{groupName} = 'Collisions11' and {datasetName} LIKE '%Prompt%' and {datasetState} = 'COMPLETED' and {bfield}>3.7 and {runNumber} >= "+RUNMINCFG+" and {runNumber} <= "+RUNMAXCFG


    RUN_DATA= server.DataExporter.export('RUN', 'GLOBAL', 'csv_runs', sel_runtable)

    BEAM_ENE_ALL=[450.0,1380.0,3500.0]
    BEAM_ENE_DEF=3500.0

    for line in RUN_DATA.split("\n"):
        
        runno=line.split(',')[0]
        
        if runno.isdigit():
            # we need to do something to avoid parsing commas in comment column
            group=line.split('",')[8]
            energy=group.split(',')[0]
            mindiff=999999.
            if energy.isdigit():
                for be in BEAM_ENE_ALL:
                    if abs(float(energy)-be)<mindiff:
                        mindiff=abs(float(energy)-be)
            if mindiff>400.0:
                print "WARNING: Something wrong with energies in run "+runno
                print "WARNING: Getting: "+energy+" from RR.Using default value of:"+str(BEAM_ENE_DEF)
                energy=str(BEAM_ENE_DEF)

            #exclude runs not at 7 TeV
            if abs(float(energy)-3500.)>400.:
                continue

            endtime=line.split(',')[2]
            # transform time in unix time
            timeinsec=time.mktime(time.strptime(endtime,'"%Y-%m-%dT%H:%M:%S.0"'))
            day_bound=endtime.split('T')[0]
            day_bound_min=time.mktime(time.strptime(day_bound+'T00:00:00"','"%Y-%m-%dT%H:%M:%S"'))
            day_bound_max=time.mktime(time.strptime(day_bound+'T23:59:59"','"%Y-%m-%dT%H:%M:%S"'))

            RUN_TIME_00[runno]=day_bound_min
            RUN_TIME[runno]=timeinsec


def definetemplates():
    global MAINPAGE_TEMPL,SCENARIO_TEMPL,SCENARIOLINE_TEMPL,PLOT_TEMPL
    global LUMILOSS_ROW_TEMPL
    
    MAINPAGE_TEMPL="""
<html>
<head>
<meta content="text/html; charset=ISO-8859-1"
http-equiv="content-type">
<title>Lumi_template</title>
</head>
<body style="background-color: rgb(255, 255, 204); color: rgb(0, 0, 0);"
alink="#ee0000" link="#0000ee" vlink="#551a8b">
<h1 style="text-align: center; color: red;">Summary of luminosity
usable for physics analysis</h1>
<div style="text-align: center; color: red;"><big><big><big>IMPORTANT:
this is a work-in-progress page and the results here are <span
style="color: rgb(0, 0, 153);">NOT public, NOT official, NOT to be
used anywhere</span>.<br>
USE IT AT YOUR OWN RISK<br>
</big></big></big></div>
<br>
<br>
Criteria: all runs in the run range below belonging to the Collisions11  group and with nominal BField and energy<br>
Latest update:&nbsp; <span style="color: rgb(0, 0, 153);">TAG_DATE</span><br>
Default JSON file used: <span style="color: rgb(0, 0, 153);"> <a href="runreg_std.cfg" target="_blank">runreg_std.cfg</a></span><br>
Run range used here: <span style="color: rgb(0, 0, 153);">TAG_RUNRANGE (i.e. up to TAG_LASTRUNDATE) </span><br>
<br>
Datasets and run range per dataset: <span style="color: rgb(0, 0, 153);">TAG_DSRUNRANGE </span><br>
<br>
Plots for single quality criteria are available in the dedicated pages:
<br>
TAG_SCENARIOLINES
<br>
<div style="text-align: center;"><big><big><big><span
style="font-weight: bold;">Collected lumi </span></big></big></big><br>
TAG_LUMIPLOT
</div>
<div style="text-align: center;"><big style="font-weight: bold;"><big><big>Lumi
losses (recorded - accepted) </big></big></big><br>
TAG_LOSSPLOT
</div>
<div style="text-align: center;"><big><big><big><span
style="font-weight: bold;">Accepted lumi w.r.t. recorded vs scenario</span></big></big></big><br>
TAG_SUMMARYPLOT
</div>
<br>
<div style="text-align: center;"><big style="font-weight: bold;"><big><big>Relative losses
(i.e. exclusive losses) </big></big></big><br>
<div style="text-align: center;"><big style="font-weight: bold;">Click on tag for details</big><br>

<table border="1" align="center">
<tr>
<td> Scenario vs Scenario </td>
<td> Lumiloss loss (pb-1) </td>
</tr>
TAG_LUMI_LOSS_REL
</table>
</div>
</body>
</html>
"""
    SCENARIOLINE_TEMPL="""
scenario TAG_SCENARIO: <a href="TAG_PAGE.html" target="_blank">results</a>, <a href="TAG_LLSUMM.png" target="_blank">lumiloss summary plot</a>, <a href="TAG_LLSUMM.txt" target="_blank">lumiloss ascii</a><br>
"""

    SCENARIO_TEMPL="""
<html>
<head>
<meta content="text/html; charset=ISO-8859-1"
http-equiv="content-type">
<title>Lumi_template</title>
</head>
<body style="background-color: rgb(255, 255, 204); color: rgb(0, 0, 0);"
alink="#ee0000" link="#0000ee" vlink="#551a8b">
<h1 style="text-align: center; color: red;">Scenario: TAG_SCENARIO</h1>
<br>
<br>
Latest update:&nbsp; <span style="color: rgb(0, 0, 153);">TAG_DATE</span><br>
<a href="lumiforphys.html" target="_blank">Go back to summary page</a> <br>

TAG_PLOT

</div>
</body>
</html>

"""

    PLOT_TEMPL="""
<div style="text-align: center;"><big><big><big><span
style="font-weight: bold;">TAG_PLOTNAME </span></big></big></big><br>
TAG_SINGLEPLOT
</div>
"""

    LUMILOSS_ROW_TEMPL="""
<tr>
<td> <a href="TAG_FILENAME" target="_blank"> TAG_FULLNAME </a> </td>
<td> TAG_LUMILOSS </td>
</tr>
"""


def makehtmlpages():
    global HTMLDIR
    global MAINPAGE_TEMPL,SCENARIO_TEMPL,SCENARIOLINE_TEMPL,PLOT_TEMPL
    global LUMILOSS_ROW_TEMPL
    global DEF_CFG
    global RUNMIN,RUNMAX,RUN_TIME
    global JSONLIST,TAG_NAME
    global NEW_DATASET_ALL
    global plot_type,plot_xtype,plot_xrange
    global REL_LOSS_VAL
    
    definetemplates()
    # replace tags and lines
    curr_time=time.asctime()
    MAINPAGE_TEMPL=MAINPAGE_TEMPL.replace("TAG_DATE",curr_time)
    # copy cfg file
    os.system('cp '+DEF_CFG+' '+HTMLDIR+'/runreg_std.cfg')

    MAINPAGE_TEMPL=MAINPAGE_TEMPL.replace("TAG_RUNRANGE",str(RUNMIN)+'-'+str(RUNMAX))
    MAINPAGE_TEMPL=MAINPAGE_TEMPL.replace("TAG_LASTRUNDATE",time.ctime(RUN_TIME[str(RUNMAX)]))

# datasets and run range
    ds_rr_replace="<br>"
    DSETS=NEW_DATASET_ALL.split()
    for DS in DSETS:
        ds_rr_replace+=DS+" <br>"

    MAINPAGE_TEMPL=MAINPAGE_TEMPL.replace("TAG_DSRUNRANGE",ds_rr_replace)

    SCENARIO_LINES=""
    for scenario in range(0,len(JSONLIST)):
        TAG_LLSUMM="lumiloss_"+TAG_NAME[scenario].replace(' ','_')
        SCENARIO_CURR=SCENARIOLINE_TEMPL.replace("TAG_SCENARIO",TAG_NAME[scenario])
        SCENARIO_CURR=SCENARIO_CURR.replace("TAG_PAGE","scenario_"+str(scenario))
        SCENARIO_CURR=SCENARIO_CURR.replace("TAG_LLSUMM",TAG_LLSUMM)
        SCENARIO_LINES+=SCENARIO_CURR

        # make up scenario pages
        SCENARIO_PAGE=SCENARIO_TEMPL
        SCENARIO_PAGE=SCENARIO_PAGE.replace("TAG_SCENARIO",TAG_NAME[scenario])
        SCENARIO_PAGE=SCENARIO_PAGE.replace("TAG_DATE",curr_time)

        PLOT_LINES=""
        for p_type in plot_type:
            for p_xtype in plot_xtype:
                TITLE="Plot of "+p_type+" vs "+p_xtype
                PLOT_CURR=PLOT_TEMPL.replace("TAG_PLOTNAME",TITLE)
                PLOTS=""
                for p_xrange in plot_xrange:
                    plotname='plot_'+p_type+'_'+p_xtype+'_'+p_xrange+'_'+str(scenario)
                    PLOTS+='<a href="'+plotname +'.png"> <img src="'+plotname +'.png" alt="'+plotname +'" width="30%" /></a>\n'
                    os.system('mv '+plotname+'.png '+HTMLDIR+'/')
                PLOT_CURR=PLOT_CURR.replace("TAG_SINGLEPLOT",PLOTS)
                PLOT_LINES+=PLOT_CURR

        SCENARIO_PAGE=SCENARIO_PAGE.replace("TAG_PLOT",PLOT_LINES)
        htmlfile=open(HTMLDIR+"/scenario_"+str(scenario)+".html",'w')
        htmlfile.write(SCENARIO_PAGE)
        htmlfile.close()

    MAINPAGE_TEMPL=MAINPAGE_TEMPL.replace("TAG_SCENARIOLINES",SCENARIO_LINES)
    # add plots to main page
    # first lumi
    PLOTLINE=""
    PLOTLEFT ='plot_lumi_time_tot_0'
    PLOTMIDDLE='plot_lumi_run_new_0'
    PLOTRIGHT='plot_lumi_run_lastweek_0'
    PLOTLINE+='<a href="'+PLOTLEFT +'.png"> <img src="'+PLOTLEFT +'.png" alt="'+PLOTLEFT +'" width="30%" /></a>\n'
    PLOTLINE+='<a href="'+PLOTMIDDLE+'.png"> <img src="'+PLOTMIDDLE+'.png" alt="'+PLOTMIDDLE+'" width="30%" /></a>\n'
    PLOTLINE+='<a href="'+PLOTRIGHT+'.png"> <img src="'+PLOTRIGHT+'.png" alt="'+PLOTRIGHT+'" width="30%" /></a>\n'
    MAINPAGE_TEMPL=MAINPAGE_TEMPL.replace("TAG_LUMIPLOT",PLOTLINE)
    # then losses
    PLOTLINE=""
    PLOTLEFT ='plot_loss_time_tot_0'
    PLOTMIDDLE='plot_loss_run_new_0'
    PLOTRIGHT='plot_loss_run_lastweek_0'
    PLOTLINE+='<a href="'+PLOTLEFT +'.png"> <img src="'+PLOTLEFT +'.png" alt="'+PLOTLEFT +'" width="30%" /></a>\n'
    PLOTLINE+='<a href="'+PLOTMIDDLE+'.png"> <img src="'+PLOTMIDDLE+'.png" alt="'+PLOTMIDDLE+'" width="30%" /></a>\n'
    PLOTLINE+='<a href="'+PLOTRIGHT+'.png"> <img src="'+PLOTRIGHT+'.png" alt="'+PLOTRIGHT+'" width="30%" /></a>\n'
    MAINPAGE_TEMPL=MAINPAGE_TEMPL.replace("TAG_LOSSPLOT",PLOTLINE)
    # then summaries
    PLOTLINE=""
    PLOTLEFT ='plot_summary_tot'
    PLOTMIDDLE='plot_summary_nr'
    PLOTRIGHT='plot_summary_lw'
    PLOTLINE+='<a href="'+PLOTLEFT +'.png"> <img src="'+PLOTLEFT +'.png" alt="'+PLOTLEFT +'" width="30%" /></a>\n'
    PLOTLINE+='<a href="'+PLOTMIDDLE+'.png"> <img src="'+PLOTMIDDLE+'.png" alt="'+PLOTMIDDLE+'" width="30%" /></a>\n'
    PLOTLINE+='<a href="'+PLOTRIGHT+'.png"> <img src="'+PLOTRIGHT+'.png" alt="'+PLOTRIGHT+'" width="30%" /></a>\n'
    MAINPAGE_TEMPL=MAINPAGE_TEMPL.replace("TAG_SUMMARYPLOT",PLOTLINE)
    os.system('mv '+PLOTLEFT+'.png '+HTMLDIR+'/')
    os.system('mv '+PLOTMIDDLE+'.png '+HTMLDIR+'/')
    os.system('mv '+PLOTRIGHT+'.png '+HTMLDIR+'/')
    # also for the lumiloss summaries
    os.system('mv lumiloss_* '+HTMLDIR+'/')
    
    # create table for relative lumilosses in main page

    allrows=""
    # build up single rows in tables
    for comparison in range(0,len(REL_LOSS)):
        pair=REL_LOSS[comparison]
        curr_sc=pair[0]
        refe_sc=pair[1]
        fullname=TAG_NAME[curr_sc]+" vs "+TAG_NAME[refe_sc]
        filename="lumiloss_rel_"+str(curr_sc)+"_"+str(refe_sc)+".txt"
        lumiloss="%5.3f" % (REL_LOSS_VAL[comparison])
        thisrow=LUMILOSS_ROW_TEMPL.replace("TAG_FILENAME",filename)
        thisrow=thisrow.replace("TAG_FULLNAME",fullname)
        thisrow=thisrow.replace("TAG_LUMILOSS",lumiloss)
        allrows=allrows+thisrow
        
    MAINPAGE_TEMPL=MAINPAGE_TEMPL.replace("TAG_LUMI_LOSS_REL",allrows)
        
    # write page
    htmlfile=open(HTMLDIR+"/lumiforphys.html",'w')
    htmlfile.write(MAINPAGE_TEMPL)
    htmlfile.close()
    print "HTML pages created in "+HTMLDIR



##############################################################################
# start main



def makelumilosssummaries():
    global TAG_NAME,REL_LOSS,REL_LOSS_VAL

#LM####################THIS MUST GO
#    # really loop on scenarios
#    for scen in range(0,len(SCENS)):
#    for scen in range(0,2):
#        (curr_tagname,curr_qf,curr_dcs)=SCENS[scen]
#        TAG_NAME.append(curr_tagname)
#LM#####################END TO GO

    print " "
    print "Making lumi loss summaries (plots + txt)....."
    tagname=[]
    for scenario in range(0,len(TAG_NAME)):
        tagname.append(TAG_NAME[scenario].replace(' ','_'))

#LM uncomments this
    analoss.ana(tagname)

    # now analyze lumilosses per scenario and make relative comparisons

    
    for pair in REL_LOSS:
        curr_sc=pair[0]
        refe_sc=pair[1]
        curr={}
        refe={}
        ll_diff={}
        
        # open current scenario and read lumilosses
        filenametxt="lumiloss_"+tagname[curr_sc]+".txt"
        filetxt=open(filenametxt,'r')
        for line in filetxt.readlines():
            runno=int(line.split()[0])
            ll=float(line.split()[1])
            curr[runno]=ll
        filetxt.close()
        # open reference scenario and read lumilosses
        filenametxt="lumiloss_"+tagname[refe_sc]+".txt"
        filetxt=open(filenametxt,'r')
        for line in filetxt.readlines():
            runno=int(line.split()[0])
            ll=float(line.split()[1])
            refe[runno]=ll
        filetxt.close()
        
        # make difference and sum
        ll_diff_sum=0.
        for run in curr.keys():        
            ll_diff[run]=refe[run]-curr[run]
            ll_diff_sum+=ll_diff[run]
        REL_LOSS_VAL.append(ll_diff_sum)
        
        # and write it ordered by lumi loss
        
        filenametxt="lumiloss_rel_"+str(curr_sc)+"_"+str(refe_sc)+".txt"
        filetxt=open(filenametxt,'w')
        sortedlist=sorted(ll_diff.items(), key=itemgetter(1),reverse=True)
        for item in sortedlist:
            (runno,lumiloss)=item
            filetxt.write(str(runno)+" "+str(lumiloss)+"\n")
        filetxt.close()

        

def main():

    global DEF_CFG,LUMI_CSV,LUMI_RESET,JSONLIST,GLOB,KEEPRUNRANGE
    global HTMLDIR,LUMICACHE,CERT_DIR


    parser=OptionParser()
    # required parameters
    parser.add_option('-c','--cfg',dest='runregcfg',default='none',help='standard runreg.cfg (required)')
    # optional parameters
    parser.add_option('-l','--lumicsv',dest='lumicsv',default='none',help='lumicsv file (optional), if not given luminosity taken from lumidb')
    parser.add_option('-r','--resetlumi',dest='resetlumi',action="store_true",default=False,help='lumi cache file is reset (it might be long)')
    parser.add_option('-g','--global',dest='central',action="store_true",default=False,help='set all paths for central running (to create global web pages)')
    parser.add_option('-k','--keeprange',dest='keeprange',action="store_true",default=False,help='keep run range in runreg.cfg')

    (options, args) = parser.parse_args()

    if options.runregcfg=='none':
        print "Please provide a runreg.cfg file"
        sys.exit()

    if options.resetlumi and options.lumicsv!='none':
        print "The resetlumi otion implies reading from lumiDB. No lumicsv file should be given."
        sys.exit(1)


    GLOB=options.central
    DEF_CFG=options.runregcfg
    LUMI_CSV=options.lumicsv
    LUMI_RESET=options.resetlumi
    KEEPRUNRANGE=options.keeprange

# htmldirectory
    if GLOB:

        HTMLDIR='/afs/cern.ch/user/m/malgeril/www/lumiforphys'
        LUMICACHE='lumicache.csv'
        CERT_DIR='/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions11/7TeV/DCSOnly'
        #CERT_DIR='/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions10/7TeV/DCSOnly'
        print "Running centrally and writing in:"
        print "HTMLDIR:   ",HTMLDIR
        print "LUMICACHE: ",LUMICACHE
        print "CERT_DIR:  ",CERT_DIR

    else:
        HTMLDIR='./htmldir'
        LUMICACHE='lumicache_local.csv'
        CERT_DIR='./'
        print "Running locally and writing in:"
        print "HTMLDIR:   ",HTMLDIR
        print "LUMICACHE: ",LUMICACHE
        print "CERT_DIR:  ",CERT_DIR

    if not os.path.exists(HTMLDIR):
        print "The diretory ",HTMLDIR," does not exist, creating it...."
        os.system("mkdir "+HTMLDIR)


    # first loop on all scenarios
    looponscenario()

    # then read the run/lumi
    if LUMI_CSV=='none':
        readlumi_db()
    else:
        readlumi()


    # final make the plots for each scenario
    for scenario in range(0,len(JSONLIST)):
        makeplot(scenario)

    makesummaryplot()
    makelumilosssummaries()
    makehtmlpages()

    ## copy the basic json (DCS all on only) in the certification area
    #ref=REPORT[1]
    # copy the basic json (DCS tracker on only) in the certification area
    ref=REPORT[len(JSONLIST)-1]
    # first read run range from json
    json_file_ref=file(ref[2],'r')
    json_dict = json.load(json_file_ref)
    # determine runmin and runmax
    RMIN=999999
    RMAX=0
    for run in json_dict.keys():
    	if int(run)>RMAX:
    		RMAX=int(run)
    	if int(run)<RMIN:
    		RMIN=int(run)
    print "Run Range in JSON file for DCS Tracker Only certification:",RMIN,"-",RMAX
    certfile_name=CERT_DIR+"/DCSTRONLY_"+str(RMIN)+"-"+str(RMAX)
    os.system("cp "+ref[2]+" "+certfile_name)
    json_file_ref.close()

if __name__ == '__main__':
    main()

