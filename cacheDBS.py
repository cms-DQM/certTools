#!/usr/bin/env python
import os, time, shutil, string, subprocess,sys, glob

outfile = "DBS2012D_cache.txt"
sdbs = 'dbs search --query="find run,lumi where dataset =\'/MinimumBias/Run2012D-v1/RAW\'  and run = RUNNUMBER order by lumi"'
srunlist = "208686 208551 208541 208540 208538 208535 208509 208487 208486 208429 208428 208427 208407 208406 208402 208397 208395 208394 208393 208392 208391 208390 208357 208353 208352 208351 208341 208339 208307 208304 208300 207924 207922 207921 207920 207905 207898 207897 207889 207887 207886 207885 207884 207883 207882 207875 207813 207790 207789 207779 207714 207518 207517 207515 207492 207491 207490 207488 207487 207477 207469 207468 207454 207398 207397 207372 207371 207328 207320 207316 207299 207279 207273 207269 207233 207231 207222 207221 207220 207219 207217 207214 207100 207099 206940 206939 206906 206901 206897 206869 206868 206867 206866 206859 206745 206744 206605 206598 206596 206595 206594 206575 206574 206573 206572 206550 206542 206539 206513 206512 206484 206478 206477 206476 206466 206448 206446 206401 206391 206390 206389 206331 206304 206303 206302 206297 206258 206257 206246 206245 206243 206210 206208 206207 206199 206188 206187 206102 206098 206088 206066 205921 205908 205834 205833 205826 205781 205777 205774 205718 205694 205690 205683 205667 205666 205627 205620 205618 205617 205614 205605 205600 205599 205598 205595 205526 205519 205515 205344 205339 205311 205310 205303 205238 205236 205233 205217 205193 205158 205111 205086 204601 204600 204599 204577 204576 204567 204566 204565 204564 204563 204555 204554 204553 204552 204551 204545 204544 204541 204511 204506 204250 204238 204114 204113 204101 204100 203994 203992 203991 203987 203986 203985 203981 203980 203912 203909 203894 203853 203835 203834 203833 203832 203830 203780 203778 203777"

#mylist = []
mylist = srunlist.split(" ")

"""
Main Script
"""
if __name__ == "__main__":
    myfile = open(outfile, "w")
    for run in mylist:
        if run.isdigit():
            thisrunlumi = run + " "
            thisProcess = sdbs.replace('RUNNUMBER', run)
            print thisProcess
            if os.path.exists("tempFile.stdout") is True: os.remove("tempFile.stdout")
            outFile = "tempFile.stdout"
            outptr = open(outFile, "w")
            if os.path.exists("tempFile.stderr") is True: os.remove("tempFile.stderr")
            errFile = "tempFile.stderr"
            errptr = open(errFile, "w")
            retval = subprocess.call(thisProcess, shell=True, stdout=outptr, stderr=errptr)
            errptr.close()
            outptr.close()
            if not retval == 0:
                errptr = open(errFile, "r")
                errData = errptr.read()
                errptr.close()
                raise Exception("Error executing command: " + thisProcess)
            else:
                print " Successed! "
                outptr = open(outFile, "r")
                outData = outptr.readlines()
                outptr.close()
                for line in outData:
                    if run in line:
                        if len(line.split()) > 1:
                            thisrunlumi += line.split()[1] + " "
            myfile.write(thisrunlumi + '\n')
    myfile.close()
