import sys,os,string,commands,time
from optparse import OptionParser
from array import array
from operator import itemgetter

def ana(tagname, verbosityLevel):
    CUT=0.1
    tag1="LossOfIntegratedLuminosity->GetXaxis()->SetBinLabel("
    tag2="graph->SetPoint("
    FILEPRE='tmp_plot_loss_run_tot_'
    
    sortedscenario=[]
    
    for scenario in range(0,len(tagname)):
        FILEIN=FILEPRE+str(scenario)+'.C'
        if verbosityLevel: print "opening file "+FILEIN
        f=open(FILEIN,'r')
        
        runlist={}
        losslist={}
        lossperrun={}
        
        for line in f.readlines():
            if tag1 in line:
                rec=line.split("SetBinLabel(")[1]
                item=int(rec.split(",")[0])-1
                val=rec.split(",")[1]
                val=val.split('"')[1]
                runlist[item]=val
            elif tag2 in line:
                rec=line.split("SetPoint(")[1]
                rec=rec.split(")")[0]
                item=int(rec.split(",")[0])
                val=abs(float(rec.split(",")[2]))
                losslist[item]=val
        
        for item in runlist.keys():
            lossperrun[runlist[item]]=losslist[item]
        
        if verbosityLevel: print lossperrun.items()
        sortedlist=sorted(lossperrun.items(), key=itemgetter(1),reverse=True)
        sortedscenario.append(sortedlist)
    
    if verbosityLevel: print sortedscenario
    from ROOT import TCanvas,TGraph,gStyle,gROOT,TH1F,TLatex,gPad

    gROOT.SetBatch(True)
    c1 = TCanvas("c1","c1",480,640)
    gStyle.SetOptStat(0)
    gStyle.SetTitleFontSize(0.03)
    for scenario in range(0,len(tagname)):
        filenametxt="lumiloss_"+tagname[scenario]+".txt"
        filetxt=open(filenametxt,'w')

        totloss=0
        plot= TH1F(tagname[scenario],tagname[scenario]+";lumi loss (pb-1);# runs",100,0.,2.)
        texlines=[]
        for item in sortedscenario[scenario]:
            (runno,lumiloss)=item
            filetxt.write(runno+" "+str(lumiloss)+"\n")
            plot.Fill(lumiloss)
            totloss+=lumiloss
            if lumiloss>CUT:
                texlines.append(runno+": "+"%3.2f"%lumiloss+"pb^{-1}")
        filetxt.close()
        if verbosityLevel: print "File: "+filenametxt+" written!"
        plot.Draw()
        gPad.SetLogy(1)
        
        linestep=-0.03
        xcoord = 0.60
        ycoord = 0.935
        tex0=[]
    
        for line in range(0,len(texlines)+2):
            if line==0: text="Total loss: "+"%3.2f"%totloss+" pb^{-1}"
            if line==1: text="Major losses (>0.1 pb^{-1}):"
            if line>1: text=texlines[line-2]
            tex0.append(TLatex(xcoord,ycoord,text))
            tex0[len(tex0)-1].SetNDC(True)
            tex0[len(tex0)-1].SetTextSize(0.03)
            tex0[len(tex0)-1].SetTextColor(1)
            tex0[len(tex0)-1].SetLineWidth(2)
            tex0[len(tex0)-1].Draw()
            ycoord+=linestep
        
        c1.Update()
        c1.Print("lumiloss_"+tagname[scenario]+".png")
