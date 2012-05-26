#!/bin/bash

period=$1

cd $1

kind=lossdcs
dqm=none
rm ${kind}.ascii

for i in `cat ${period}.runlist`; do 
 for dcs in all_on Csc_on Dt_on Ecal_on Es_on HCAL_on Rpc_on Strip_on Pix_on; do
 awk '{if ($1=="'"$i"'" && $2>=0.001) {print $1, $2, "'"${dcs}"'"}}' lumiloss_DQM\:_${dqm}\,_DCS\:_${dcs}.txt >> ${kind}.ascii
 done
 #dcs
done
#i


kind=lossdqm 
dcs=none
rm ${kind}.ascii

for i in `cat ${period}.runlist`; do 
 for dqm in Egam Hlt Jmet L1t Lumi Muon Track ; do
 awk '{if ($1=="'"$i"'" && $2>=0.001) {print $1, $2, "'"${dqm}"'"}}' lumiloss_DQM\:_${dqm}\,_DCS\:_${dcs}.txt >> ${kind}.ascii
 done
 #dqm
done
#i



kind=losscombined
rm ${kind}.ascii

for i in `cat ${period}.runlist`; do
 for dqm in Ecal Hcal Es Csc Dt Strip Pix; do
 dcs=${dqm}
 awk '{if ($1=="'"$i"'" && $2>=0.001) {print $1, $2, "'"${dqm}"'"}}' lumiloss_DQM\:_${dqm}\,_DCS\:_${dcs}.txt >> ${kind}.ascii
 done
 #dqm
done
#i   



kind=lossgolden
rm ${kind}.ascii

for i in `cat ${period}.runlist`; do
 for dqm in all ; do
 dcs=all_on
 awk '{if ($1=="'"$i"'" && $2>=0.001) {print $1, $2, "'"${dqm}"'"}}' lumiloss_DQM\:_${dqm}\,_DCS\:_${dcs}.txt >> ${kind}.ascii
 done
 #dqm
done
#i   



file=log`date +%d%b%Y-%H:%M`-period${period}

cat >> $file <<EOF
Writing logfile $file

DCS losses:
`awk '{$3=="all_on" && x += $2}; END {print "all= " x}' lossdcs.ascii`
`awk '{$3=="Strip_on" && x += $2}; END {print "SiStrip= " x}' lossdcs.ascii`
`awk '{$3=="Pix_on" && x += $2}; END {print "Pixel= " x}' lossdcs.ascii`
`awk '{$3=="Csc_on" && x += $2}; END {print "CSC= " x}' lossdcs.ascii`
`awk '{$3=="Dt_on" && x += $2}; END {print "DT= " x}' lossdcs.ascii`
`awk '{$3=="HCAL_on" && x += $2}; END {print "Hcal= " x}' lossdcs.ascii`
`awk '{$3=="Ecal_on" && x += $2}; END {print "Ecal= " x}' lossdcs.ascii`
`awk '{$3=="Es_on" && x += $2}; END {print "ES= " x}' lossdcs.ascii`
`awk '{$3=="Rpc_on" && x += $2}; END {print "RPC= " x}' lossdcs.ascii`

DQM losses:
`awk '{$3=="Lumi" && x += $2}; END {print "Lumi= " x}' lossdqm.ascii`
`awk '{$3=="Hlt" && x += $2}; END {print "Hlt=  " x}' lossdqm.ascii`
`awk '{$3=="L1t" && x += $2}; END {print "L1t=  " x}' lossdqm.ascii`

Combined DQM && DCS:
`awk '{$3=="Hcal" && x += $2}; END {print "Hcal= " x}' losscombined.ascii`
`awk '{$3=="Ecal" && x += $2}; END {print "Ecal= " x}' losscombined.ascii`
`awk '{$3=="Es" && x += $2}; END {print "ES = " x}' losscombined.ascii`
`awk '{$3=="Csc" && x += $2}; END {print "CSC= " x}' losscombined.ascii`
`awk '{$3=="Dt" && x += $2}; END {print "DT = " x}' losscombined.ascii`
`awk '{$3=="Strip" && x += $2}; END {print "Strip= " x}' losscombined.ascii`
`awk '{$3=="Pix" && x += $2}; END {print "Pix= " x}' losscombined.ascii`




`awk '{$3=="all" && x += $2}; END {print "Sum of losses in Golden JSON= " x}' lossgolden.ascii`

runs:
`cat ${period}.runlist` 

EOF
