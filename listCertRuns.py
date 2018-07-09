#!/usr/bin/env python

# before running script set-up brilcalc:
# export PATH=$HOME/.local/bin:/afs/cern.ch/cms/lumi/brilconda-1.1.7/bin:$PATH
# technically we can stick with bril's python if not cmsenv

###############################################################
# weekly use
# python py_files/listCertRuns_tempVIR.py --min=min_runNumber
# Low B field (< 3.7)
# python py_files/listCertRuns_tempVIR.py --min=279850 --Bfield
# special HLT key
# python py_files/listCertRuns_tempVIR.py --min=279850 --HLT
###############################################################
import sys
import csv
import logging
import smtplib
import datetime
import argparse
import subprocess

from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate

from rhapi import RhApi

def to_useful_format(in_data):
    """
    converts list of reuslt columns to {key:[result]} where key is run
    """
    new_format = {}
    for el in in_data["data"]:
        new_format[el[1]] = el

    return new_format

def parse_brilshit_to_run_recorded(data):
    ret_data = []
    rows = []
    reader = csv.reader(data)
    for row in reader:
        rows.append(row)

    for el in rows[2:]:
        if "#Summary:" in el:
            break
        else:
            return float(el[-1])

def run_brilcalc(run):
    """
    Run brilcalc in separate subprocess
    return it's output split by line
    """
    __brilcalc_args = ["brilcalc", "lumi",  "-b", "STABLE BEAMS",
            "-r", "%s" % (run) ,"-u", "/nb", "--output-style=csv"]

    logging.debug("brilcalc command: %s" % (__brilcalc_args))
    p = subprocess.Popen(__brilcalc_args, stdout=subprocess.PIPE)
    out = p.communicate()[0]
    logging.debug("brilcalc output: %s" % (out))

    return out.split("\n")

def checkRR_sync():
    """
    check if synchronisation from OfflineRR -> UsersRR happened more than 6h ago
    """
    __query = ("select * from runreg.synch s")
    logging.info("Checking RR sync time for online->offline, offline->users")
    logging.debug("RR query: %s" % (__query))

    try:
        rr_data = api.json(__query)
    except Exception as ex:
        logging.error("Error while using RestHub API: %s" % (ex))
        sys.exit(-1)

    logging.debug("RRData: %s" % (rr_data))
    # online->offline is 0, offline->users is 1 lets hope it stays like this
    # hope RR doesn't change dateformat too...
    online2offline = rr_data["data"][0][0]
    offline2users = rr_data["data"][0][1]
    now = datetime.datetime.now()

    if now - datetime.timedelta(hours=6) < datetime.datetime.strptime(online2offline,
            "%Y-%m-%d %H:%M:%S"):
        logging.error("Synchronisation online to offline was finished 6+H ago")

    if now - datetime.timedelta(hours=6) < datetime.datetime.strptime(offline2users,
            "%Y-%m-%d %H:%M:%S"):
        logging.error("Synchronisation offline to users was finished 6+H ago")

def getRR2(dataset_name, run_class_name, columns=[], dataset_state='',
        hlt_key='/cdaq/physics/', special=""):

    """
    query RR using resthubAPI to get list of runs
    dataset_name: Online,Prompt
    run_class_name: Collisions, Cosmics
    dataset_state: optional value for RunRegistry's state value OPEN,SINGOFF
    """
    logging.info("Querying RestHub API")
    # we do a joing for datasets and runs table
    __sql_columns = ["d.rda_wor_name", "d.run_number"]
    for el in columns:
        logging.debug("adding a column: %s" % (el))
        __sql_columns.append("d.%s" % (el))

    if special == "bfield":
        secondary_filter = "and r.bfield <= 3.7 "
    elif special == "hlt":
        secondary_filter = "and r.hltkeydescription like '%s%%' " % (hlt_key)
    else:
        secondary_filter = "and r.bfield > 3.7 and r.hltkeydescription like '%s%%' " % (
                hlt_key)

    __query = ("select %s "
            "from runreg_global.datasets d, runreg_global.runs r "
            "where d.run_class_name like '%%%s%%' and "
            "d.run_number >= %s and d.rda_name like '%%%s%%' "
            "%s"
            "and r.runnumber = d.run_number") % (",".join(__sql_columns),
                    run_class_name, options.min, dataset_name, secondary_filter)

    logging.debug("RR query: %s" % (__query))
    try:
        rr_data = api.json(__query)
    except Exception as ex:
        logging.error("Error while using RestHub API: %s" % (ex))
        sys.exit(-1)

    #logging.debug("RR return data: %s" % (rr_data))
    data = to_useful_format(rr_data)
    return data

def sendMail(messageText, emailSubject, **kwargs):
    """
    send textmessage from hardcoded email to some recipients over SMTP
    """
    msg = MIMEMultipart()
    send_from = "anorkus@cern.ch"
    msg['From'] = send_from
    send_to = ["anorkus@gmail.com"]#, "azzolini@cern.ch"]
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = emailSubject
    try:
        msg.attach(MIMEText(messageText))
        smtpObj = smtplib.SMTP()
        smtpObj.connect()
        smtpObj.sendmail(send_from, send_to, msg.as_string())
        smtpObj.close()
    except Exception as e:
        logging.erro("Error: unable to send email: %s" % (e.__class__))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description='Get a list of runs for users to certify',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument("-v", "--verbose", dest="verbose", action="store_true",
            default=False, help="Print more info")
    parser.add_argument("-B", "--Bfield", dest="verboseBfield", action="store_true",
            default=False, help="Display more information in bField method")
    parser.add_argument("-H", "--HLT", dest="verboseHLT", action="store_true",
            default=False, help="Display more information in HLT method")
    parser.add_argument("-m", "--min", dest="min", type=int, default=268000,
            help="Minimum run")
    parser.add_argument("-M", "--max", dest="max", type=int, default=999999,
            help="Maximum run")
    parser.add_argument("-g", "--group", dest="group", type=str,
            default="Collisions18", help="Run class type: Collisions17/Cosmics18 etc.")
    parser.add_argument("--sendmail", dest="send_mail", action="store_true",
            default=True, help="Send email with run list")

    options = parser.parse_args()

    if options.verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logging.basicConfig(format='[%(asctime)s] [%(levelname)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S', level=log_level)

    logging.debug("verbose:%s" % (options.verbose))
    runs_data = {}

    new_url = "http://vocms00170:2113"
    dev_url = "http://vocms0185/rhapi"
    api = RhApi(dev_url, debug=False)

    checkRR_sync()

    runs_data["online"] = sorted(getRR2("Online", options.group).keys())
    runs_data["offline"] = getRR2("PromptReco/", options.group, columns=["rda_state"])
    runs_data["lowbfield"] = getRR2("Online", options.group, special="bfield")
    runs_data["specialHLT"] = getRR2("Online", options.group, hlt_key="/cdaq/special",
            special="hlt")

    __runs_completed = []
    __runs_signoff = []
    __runs_open = []
    __short_run = []
    __short_run_online = []
    __short_run_offline = []
    __runs_tobe_certified = []

    # iterate onprevious gotten runs and get those with needed state
    for el in runs_data["offline"]:
        # 316928: [u'GLOBAL', 316928, u'COMPLETED']
        if runs_data["offline"][el][2] == "COMPLETED":
            __runs_completed.append(el)
        elif runs_data["offline"][el][2] == "SIGNOFF":
            __runs_signoff.append(el)
        elif runs_data["offline"][el][2] == "OPEN":
            __runs_open.append(el)
        else:
            logging.error("Strange dataset state for run: %s: %s" % (
                    el, runs_data["offline"][el][2]))

    logging.info("Online runs: %s" % (sorted(runs_data["online"])))
    logging.info("Offline runs in state COMPLETED: %s" % (sorted(__runs_completed)))
    logging.info("Offline runs in state SIGNOFF: %s" % (sorted(__runs_signoff)))
    logging.info("Offline runs in state OPEN: %s" % (sorted(__runs_open)))
    logging.info("Runs with bField <=3.7 : %s" % (
            sorted(runs_data["lowbfield"].keys())))

    logging.info("Runs with special HLT menu: %s" % (
            sorted(runs_data["specialHLT"].keys())))

    if options.verboseBfield:
        logging.info("Calculate lumi for lowBfield runs")
        for run in runs_data["lowbfield"]:
            out = run_brilcalc(el)
            run_reclumi = parse_brilshit_to_run_recorded(out)
            logging.info("Run %s recorded lumi: %s" % (el, run_reclumi))


    if options.verboseHLT:
        logging.info("Calculate lumi for special HLT runs")
        for run in runs_data["specialHLT"]:
            out = run_brilcalc(el)
            run_reclumi = parse_brilshit_to_run_recorded(out)
            logging.info("Run %s recorded lumi: %s" % (el, run_reclumi))

    runs_online_only = [
        run for run in runs_data["online"] if run not in runs_data["offline"]]

    logging.info("runs in Online RR which are not in Offline RR: %s" % (
            runs_online_only))

    runs_not_completed = __runs_signoff + __runs_open + runs_online_only
    logging.info("Runs in 'OPEN' Offline + 'SIGNOFF' Offline + Online Only: %s " % (
        runs_not_completed))

    # TO-DO: add checkLumi option. in case we do not check display
    logging.info("Will check luminosity for runs not in complete status")

    for el in runs_not_completed:
        out = run_brilcalc(el)
        run_reclumi = parse_brilshit_to_run_recorded(out)
        logging.info("Run %s recorded  %s" % (el, run_reclumi))

        if run_reclumi - 80.0 <= 0.0:
            if options.verbose:
                logging.debug("Short run: %s" % (el))
            __short_run.append(el)

    __short_run_online = [run for run in __short_run if run in runs_online_only]

    for run in __short_run:
        if run in runs_data["offline"] and run in __runs_signoff:
            __short_run_offline.append(run)

    for run in runs_not_completed:
        if (run not in __short_run and
                run not in __runs_signoff and
                run not in runs_online_only):

            __runs_tobe_certified.append(run)

    logging.info("These are %s runs to be certified: %s" % (
            len(__runs_tobe_certified), __runs_tobe_certified))

    logging.info("There are %s short runs (online+offline): %s" % (
            len(__short_run), __short_run))

    logging.info("There are %s short runs in offline RR: %s" % (
        (len(__short_run_offline), __short_run_offline)))

    logging.info("There are %s short runs in online RR: %s" % (
        (len(__short_run_online), __short_run_online)))

    __long_runs_signoff = [run for run in __runs_signoff if run not in __short_run]
    if len(__long_runs_signoff):
        logging.info("There are %s runs: %s in signoff today (Not short runs)" % (
                len(__long_runs_signoff), __long_runs_signoff))

    __long_runs_online = [run for run in runs_online_only if run not in __short_run]
    if len(__long_runs_online) > 0:
        logging.info("There are %s runs: %s in online today (Not short runs)" % (
                len(__long_runs_online), __long_runs_online))

    # write runs-to-be certified to a file
    with open("Collisions18.txt", "w") as f:
        f.write("\n".join([str(run) for run in __runs_tobe_certified]))

    logging.info("Will send email for runs to-be certified")
    cert_deadline = datetime.datetime.now() + datetime.timedelta(days=7)
    EMAIL_SUBJECT = "Certification of %s runs: deadline %s" % (
            datetime.datetime.now().year,
            cert_deadline.strftime("%Y-%m-%d"))

    msg = ("Dear certification experts,\n"
            "This is automated message\n"
            "for the call for summary report about these runs:\n"
            "%s\n"
            "Deadline is %s 23:59 Geneva time\n"
            "The status of data certification is available at:\n"
            "https://cms-service-dqm.web.cern.ch/cms-service-dqm/"
            "CAF/certification/Collisions18/13TeV/RRComparison/status.Collisions18.html\n"
            "and is updated every hour.\n"
            "Best regards,\n"
            "    Antanas (for DQM-DC group)") % (
                    " ".join([str(run) for run in __runs_tobe_certified]),
                    cert_deadline.strftime("%Y-%m-%d"),)

    if options.send_mail
        sendMail(msg, EMAIL_SUBJECT)
