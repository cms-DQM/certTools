#!/usr/bin/env python

# generating lumi Loss plots for subdetectors. needs brilcalc BEFORE cmssw
# export PATH=$HOME/.local/bin:/afs/cern.ch/cms/lumi/brilconda-1.1.7/bin:$PATH
# cmsenv

import os
import json
import argparse
import logging
import subprocess
import sys
import csv
import jinja2
import datetime

from shutil import copy2, move
from matplotting import make_barchart, make_pizzachart

def render(tpl_path, context):
    path, filename = os.path.split(tpl_path)
    return jinja2.Environment(
        loader=jinja2.FileSystemLoader(path or './')
    ).get_template(filename).render(context)

def get_brilcalc_lumi(in_file):
    # lets pray even more that brilcalc output doesn't change
    __args = ["brilcalc", "lumi", "-b", "STABLE BEAMS", "-i",
            in_file, "--output-style=csv"]

    #bricalc has normtag parameters added
    if args.normtag:
        logging.info("Adding normtag values to brilcalc")
        __args.append("--without-checkjson")
        __args.append("--normtag")
        __args.append("/cvmfs/cms-bril.cern.ch/cms-lumi-pog/Normtags/normtag_PHYSICS.json")

    logging.debug("brilcalc params: %s" % (" ".join(__args)))
    p = subprocess.Popen(__args, stdout=subprocess.PIPE)
    out = p.communicate()[0]

    logging.debug("brilcalc output: %s" % (out))
    # should be formatted like this:
    # ['#nfill', 'nrun', 'nls', 'ncms', 'totdelivered(/ub)', 'totrecorded(/ub)']
    return out.split("\n")

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
            shit = "%s %s " % (el[0].split(":")[0], float(el[-1])/1000000)
            ret_data.append(shit)

    return ret_data

def run_compareJSON(directory, file1, file2, out_fname, method="--sub"):
    logging.debug("running compareJSON.py %s %s %s" % (file1, file2, out_fname))
    p = subprocess.Popen(["compareJSON.py", method, os.path.join(directory, file1),
                    os.path.join(directory, file2), os.path.join(directory, out_fname)],
                    stdout=subprocess.PIPE)

    out = p.communicate()[0]
    ## TO-DO: check the exit code incase it failed!

def check_and_prepare(directory, current_year):
    """
    check if output and input directory/files exists
    copy the input files to working dir
    """
    if not os.path.exists(directory):
        os.makedirs(directory)
    else:
        logging.error("Output directory: %s already exists" % (directory))
        sys.exit(-1)

    json_dir = 'scenarios_json_%s' % (current_year)
    if not os.path.exists(json_dir):
        logging.error("JSON input directory %s doesn't exists" % (json_dir))
        sys.exit(-1)

    logging.info("Copy scenarios JSON files to our working dir")
    # lets copy the previously done scenarios json files to out working dir
    for file in os.listdir(json_dir):
        if file.endswith(".txt"):
            copy2("%s/%s" % (json_dir, file), directory)

def get_total_lumi_lost():
    """
    get total lumi_lost from json_0 and json_2
    """
    # Creating the JSON file with all the losses
    p = subprocess.Popen(["compareJSON.py", "--sub",
            os.path.join(directory, "json_2.txt"),
            os.path.join(directory, "json_0.txt"),
            os.path.join(directory, "totlosses.txt")],
            stdout=subprocess.PIPE)

    # wait for subprocess to finish
    out = p.communicate()[0]
    ##Debug shit
    copy2(os.path.join(directory, "totlosses.txt"),
            os.path.join(directory, "totlosses_org.txt"))

    __total_lumi_lost = float(get_brilcalc_lumi(
            os.path.join(directory, "totlosses.txt"))[brilcalc_index].split(",")[-1])

    logging.info(("total_lumi_lost: %s (/ub),"
            " calculate 0.000001*totlumilostnum+reclumilostComm: %f (/pb)") % (
                __total_lumi_lost, (0.000001*__total_lumi_lost+reclumilostComm)))

    # i have no clue why
    if __first_run == 294927:
        __total_lumi_lost = 0.000001 * __total_lumi_lost + reclumilostComm
    else:
        __total_lumi_lost = 0.000001 * __total_lumi_lost

    logging.info("Total lumi lost: %.1f" % (__total_lumi_lost))
    return __total_lumi_lost

def calculate_delivered_lumi_and_eff(__totlumilost):
    """
    Calculating total_losses using brilcalc
    Using json_2, json_1, json_0 files for delivered, recorded lumi, efficiencies
    Store info in dictionary which will be rendered in the end of script
    """
    # calculate delivered: -1 is last field in brilcalc field
    # we get luminosity for total delivered and recorded from same file
    logging.info("Calculate total and delivered luminosity from json_2.txt")
    __tmp_json2_brilcalc = get_brilcalc_lumi(
            os.path.join(directory, "json_2.txt"))[brilcalc_index].split(",")

    __delivered_lumi = float(__tmp_json2_brilcalc[-2])
    __recorded_lumi = float(__tmp_json2_brilcalc[-1])
    __dcs_good_lumi = 0.000001 * float(get_brilcalc_lumi(
        os.path.join(directory, "json_1.txt"))[brilcalc_index].split(",")[-1])

    __cert_lumi = 0.000001 * float(get_brilcalc_lumi(
            os.path.join(directory, "json_0.txt"))[brilcalc_index].split(",")[-1])

    # some number deflation
    # some more IFs which i have no clue
    if __first_run == 294927:
        __delivered_lumi = 0.000001 * __delivered_lumi + dellumilostComm
        __recorded_lumi = 0.000001 * __recorded_lumi + dellumilostComm
    else:
        __delivered_lumi = 0.000001 * __delivered_lumi
        __recorded_lumi = 0.000001 * __recorded_lumi

    __data_taking_eff = 100 * __recorded_lumi / __delivered_lumi
    __data_cert_eff = 100 * __cert_lumi / __recorded_lumi
    __dcs_good_eff = 100 * __dcs_good_lumi / __recorded_lumi
    __golden_vs_dcs_eff = 100 *__cert_lumi / __dcs_good_lumi

    data_to_render["part1"]["runs"] = (__first_run, __last_run)
    data_to_render["part1"]["delivered_lumi"] =__delivered_lumi
    data_to_render["part1"]["recorded_lumi"] =__recorded_lumi
    data_to_render["part1"]["dcs_good_lumi"] =__dcs_good_lumi
    data_to_render["part1"]["certified_as_good"] = __cert_lumi
    data_to_render["part1"]["total_lumi_lost"] = __totlumilost
    data_to_render["part1"]["data_taking_eff"] = __data_taking_eff
    data_to_render["part1"]["data_cert_eff"] = __data_cert_eff
    data_to_render["part1"]["dcs_good_eff"] = __dcs_good_eff
    data_to_render["part1"]["golden_vs_dcs_eff"] = __golden_vs_dcs_eff

    logging.info("Delivered lumi: %.1f" % (__delivered_lumi))
    logging.info("Recorded lumi: %.1f" % (__recorded_lumi))
    logging.info("DCS good lumi: %.1f" % (__dcs_good_lumi))
    logging.info("Certified as good: %.1f" % (__cert_lumi))
    # efficiencies
    logging.info("Data taking eff: %.1f" % (__data_taking_eff))
    logging.info("DCS good eff: %.1f" % (__dcs_good_eff))
    logging.info("Golden vs DCS eff: %.1f" % (__golden_vs_dcs_eff))
    # data certification efficiency is same value as cetified_eff
    logging.info("Certified eff: %.1f" % (__data_cert_eff))


def filter_input_json():
    """
    copy input files to have unfiltered
    run filterJSON.py to filter out by specified runs in arguments
    """

    ## TO-DO: launch multiple subprocesses for spead-up! and wait until completion
    logging.info("filter the scenario JSON files")
    for i in xrange(26):
        in_file = "json_%s.txt" % (i)
        # TO-DO: do we anywhere use unfiltered files?
        copy2(os.path.join(directory, in_file),
                os.path.join(directory, "unfiltered_json_%s.txt" % (i)))

        filter_command = ["filterJSON.py", os.path.join(directory, in_file),
                "--output=%s" % (os.path.join(directory, in_file))]

        filter_command.append("--min=%s " % (args.min_run))
        if args.max_run:
            filter_command.append("--max=%s" % (args.max_run))

        if args.exclude_runs:
            filter_command.append("--runs=%s" % (",".join(args.exclude_runs)))

        logging.debug("running filter command: %s" % (" ".join(filter_command)))

        p = subprocess.Popen(filter_command, stdout=subprocess.PIPE)
        out = p.communicate()[0]

def total_lumi_loss_by_run():
    """
    calculate lumi lost for each run
    """
    logging.info("Calculating total lumi loss by Run")

    for i in xrange(16):
        __computed_lumi = 0
        if __detector_index[i] != "Mixed":
            file1 = "totlosses.txt"
            file2 = "json_%s.txt" % (__detector_fnames[i])
            out_fname = "totloss%s.txt" % (__detector_index[i])

            #TO-DO: this could also be parralelized
            p = subprocess.Popen(["compareJSON.py", "--sub",
                    os.path.join(directory, file1),
                    os.path.join(directory, file2),
                    os.path.join(directory, out_fname)], stdout=subprocess.PIPE)

            out = p.communicate()[0]

            __lumiout = 0.0
            # compareJSON.py check if compare made us non empty file
            # if its empty -> not calculate brilcalc
            __tmp_file = json.loads(open(os.path.join(directory, out_fname)).read())
            fname = "totlumilostby%sByRunLoss.txt" % (__detector_index[i])
            if __tmp_file != {}:
                brilout = get_brilcalc_lumi(os.path.join(directory, out_fname))
                __lumiout = float(brilout[brilcalc_index].split(",")[-1])

                with open((os.path.join(directory, fname)), "wb") as f:
                    f.write("%s" % ("\n".join(parse_brilshit_to_run_recorded(brilout))))
            else:
                with open((os.path.join(directory, fname)), "wb") as f:
                    f.write("")

            logging.debug("lumiout for %s is: %s" % (__detector_index[i], __lumiout))
            # more inflation
            if __lumiout > 0.0:
                __lumiout = 0.001*0.001*__lumiout
                lumi_loss_data.append((__detector_index[i], __lumiout))

            __tmp_jinja2_data = {"det_name": __detector_index[i]}
            __tmp_jinja2_data["inclusive_fname"] = "totlumilostby%sByRunLoss.txt" % (
                    __detector_index[i])

            __tmp_jinja2_data["inclusive_value"] = __lumiout
            data_to_render["part2"].append(__tmp_jinja2_data)

            with open((os.path.join(directory, "TotDataLoss.txt")), "a") as f2:
                f2.write("%s %.1f\n" % (__detector_index[i], __lumiout))
        else:
            __tmp_jinja2_data = {"det_name": __detector_index[i]}
            __tmp_jinja2_data["inclusive_fname"] = "totlumilostby%sByRunLoss.txt" % (
                    __detector_index[i])

            __tmp_jinja2_data["inclusive_value"] = 0.0
            data_to_render["part2"].append(__tmp_jinja2_data)

def calculate_exclusive_losses():
    """
    calculate exclusive losses by running compareJSON to all files expect itself
    """
    logging.info("For first 8 subdetectors exclude losses from others")
    # example: DT-(all others in definition)
    for i, el in enumerate(__detector_index):
        ## we skip Mixed directory for strange reasons
        # TO-DO: remove Mixed from the list/add it to the end
        if el == 'Mixed':
            continue
        __2nd_file = 0
        for j in xrange(8):
            # if its first iteration: we take first file from
            # totloss[DETECTOR] -> from previous loop

            if j == i:
                # we dont check itself
                continue
            __2nd_file += 1
            if __2nd_file <= 1:
                f1 = "totloss%s.txt" % (__detector_index[i])
            else:
                # -1 so we take previously generated file
                f1 = "lossdet%s%s.txt" % (i, __2nd_file-1)

            # specific condition for 0,1 element ->
            # it has to take CSC input file not 01!
            if i == 0 and j == 1:
                f1 = "totloss%s.txt" % (__detector_index[i])

            f2 = "totloss%s.txt" % (__detector_index[j])
            f3 = "lossdet%s%s.txt" % (i, __2nd_file)
            logging.debug("%s - %s = %s" % (f1, f2, f3))
            run_compareJSON(directory, f1, f2, f3)

        # run compare with last file without Tracker losses
        run_compareJSON(directory, f3, "json_TRKOff.txt", f3)

        # do some useless movement
        if i < 8:
            logging.debug("mv %s excloss%s.txt" % (f3, __detector_index[i]))
            move(os.path.join(directory, f3),
                    os.path.join(directory,"excloss%s.txt" % (__detector_index[i])))
        else:
            logging.debug("mv %s tempexcloss%s.txt" % (f3, __detector_index[i]))
            move(os.path.join(directory, f3),
                    os.path.join(directory, "tempexcloss%s.txt" % (__detector_index[i])))

    logging.info("Removing lossdet*.txt files")
    for file in os.listdir(directory):
        if file.startswith("lossdet"):
            os.remove("%s/%s" % (directory, file))

    logging.info("For all other subdetectors exclude losses from others")
    # we do same thing for POG
    for i in xrange(8, 16):
        if i == 11:
            continue
        __2nd_file = 0
        for j in xrange(8, 16):
            if j == i:
                continue
            # if it is mixed category we ignore
            if j == 11:
                continue
            __2nd_file += 1
            # if its first file in loop we take other input
            if __2nd_file <= 1:
                f1 = "tempexcloss%s.txt" % (__detector_index[i])
            else:
                # -1 so we take previously generated file
                f1 = "tempexclossdet%s%s.txt" % (i, __2nd_file-1)

            f2 = "tempexcloss%s.txt" % (__detector_index[j])
            f3 = "tempexclossdet%s%s.txt" % (i, __2nd_file)
            run_compareJSON(directory, f1, f2, f3)

        logging.debug("Exclusive data loss for %s is %s " % (
                __detector_index[i], "excloss%s.txt" % (__detector_index[i])))

        move(os.path.join(directory, f3),
                os.path.join(directory, "excloss%s.txt" % (__detector_index[i])))

def calculate_mixed_losses():
    """
    get mixed losses by taking totlosses for input
    """
    logging.info("Calculating Mixed category")
    for i in xrange(16):
        # skip mixed itself
        if i == 11:
            continue
        if i < 1:
            file1 = "totlosses.txt"
        else:
            if i == 12:
                file1 = "mixloss%s.txt" % (i - 2)
            else:
                file1 = "mixloss%s.txt" % (i - 1)

        file2 = "excloss%s.txt"  % (__detector_index[i])
        file3 = "mixloss%s.txt" % (i)
        run_compareJSON(directory, file1, file2, file3)

    run_compareJSON(directory, "mixloss15.txt", "json_TRKOff.txt", "exclossMixed.txt")

def calculate_exclussie_loss_by_run():
    """
    use brilcalc fo calculate lumi lost for each subdetector
    """
    logging.info("Use brilcalc for exclusive losses and save info to files")

    for i in xrange(16):

        __lumiout = 0.0
        out_fname = "excloss%s.txt" % (__detector_index[i])

        __tmp_file = json.loads(open(os.path.join(directory, out_fname)).read())
        fname = "exclumilostby%sByRunLoss.txt" % (__detector_index[i])
        if __tmp_file != {}:
            # write to a file brilcal output as it will be linked for each subdetector
            brilout = get_brilcalc_lumi(os.path.join(directory, "excloss%s.txt" % (
                    __detector_index[i])))

            with open((os.path.join(directory, fname)), "wb") as f:
                f.write("%s" % ("\n".join(parse_brilshit_to_run_recorded(brilout))))

            __lumiout = float(brilout[brilcalc_index].split(",")[-1])

        else:
            with open((os.path.join(directory,fname)), "wb") as f:
                f.write("")

        if __lumiout > 0.0:
            __lumiout = 0.001 * 0.001 * __lumiout

        data_to_render["part2"][i]["exclusive_fname"] = fname
        data_to_render["part2"][i]["exclusive_value"] = __lumiout

        logging.debug("Loss for: %s %.1f" % (__detector_index[i], __lumiout))
        __exclusive_detector_losses[__detector_index[i]] = __lumiout

        with open((os.path.join(directory, "DataLoss.txt")), "a") as f2:
            f2.write("%s %.1f\n" % (__detector_index[i], __lumiout))

def calculate_trk_HV_losses():
    #Calculate Losses for tracker HV transition
    logging.info("Calculating TRK_HV losses")
    lumilostbyTRK_HV = get_brilcalc_lumi(os.path.join(directory, "json_TRKOff.txt"))
    lumiout = float(lumilostbyTRK_HV[brilcalc_index].split(",")[-1])

    computed_lumi = 0
    if lumiout > 0.0:
        computed_lumi = 0.001 * 0.001 * lumiout
    else:
        computed_lumi = 0.0

    logging.debug("Computed lumi for Tracker HV transition %s" % (computed_lumi))
    __exclusive_detector_losses["TK_HV"] = computed_lumi
    with open((os.path.join(directory, "DataLoss.txt")), "a") as f2:
        f2.write("TK_HV %.1f\n" % (computed_lumi))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description='Make lumi loss plots for each sub-detector',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('-d',
            dest='directory', action='store', type=str, default='', required=True,
            help='Name of output directory for plots')
    parser.add_argument('--min',
            dest='min_run', action='store', type=int, default=1,
            help='Minimal run from which we start counting lumi losses')
    parser.add_argument('--max',
            dest='max_run', action='store', type=int, default=999999,
            help=('Max run up to which we calculate losses.'
                  'Nothing means counting to current max run'))
    parser.add_argument('--exclude', nargs='+', dest='exclude_runs',
            help='Spaced list of runs to be excluded')
    parser.add_argument('--normtag',  dest='normtag', action='store_true',
            default=False, help='Include normtg option to brilcalc')
    parser.add_argument('--hv',  dest='highvoltage', action='store_true',
            default=False, help='Calculate losses for high voltage only')
    parser.add_argument('--dqm_only',  dest='dqm_only', action='store_true',
            default=False, help='Calculate losses for DQM only')
    parser.add_argument("-v", "--verbose",
            dest="verbose", action="store_true", default=False,
            help="Print more info")

    args = parser.parse_args()

    if args.verbose:
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logging.basicConfig(format='[%(levelname)s] %(message)s', level=log_level)
    logging.debug("args: %s" % (args))

    data_to_render = {"part1": {}, "part2": []}
    lumi_loss_data = []
    __exclusive_detector_losses = {}
    __detector_fnames = [9, 8, 19, 20, 18, 22, 10, 21, 23, 24, 25, 99, 13, 12, 11, 14]
    __detector_index = ['CSC', 'DT', 'ECAL', 'ES', 'HCAL', 'Pix', 'RPC', 'Strip',
            'HLT', 'L1t', 'Lumi', 'Mixed', 'Egamma', 'JetMET', 'Muon', 'Track']

    current_year = datetime.datetime.today().year
    if args.normtag:
        # define last line index in brilcalc output. normtag was 2 lines less
        brilcalc_index = -3
        directory = "%s/Loss-%s_%s" % (current_year, args.directory, "normtag")
    else:
        brilcalc_index = -5
        directory = "%s/Loss-%s" % (current_year, args.directory)

    # some  default numbers
    reclumilostComm = 653.125
    dellumilostComm = 942.996
    logging.debug("reclumilostComm: %s /pb dellumilostComm: %s /pb" % (
            reclumilostComm, dellumilostComm))

    check_and_prepare(directory, current_year)
    filter_input_json()

    ## WHY we take info from json_2? why not from command_list args or other file?
    with open(os.path.join(directory, 'json_2.txt')) as js_f2:
        __json2_file = json.loads(js_f2.read())
        __first_run = min(__json2_file.keys())
        __last_run = max(__json2_file.keys())

    logging.info("from json_2.txt first_run: %s last_run: %s" % (
            __first_run, __last_run))

    logging.info("list of analysed runs: %s" % (sorted(__json2_file.keys())))

    with open(os.path.join(directory, 'analysed_runs.txt'), 'w') as runs_f2:
        f2.write(json.dumps(sorted(__json2_file.keys()), indent=4))

    total_lumiloss = get_total_lumi_lost()
    calculate_delivered_lumi_and_eff(total_lumiloss)
    total_lumi_loss_by_run()

    # sort the list in ascending order
    # so the subdetector with most loses will be plotted first
    lumi_loss_data.sort(key=lambda tup: tup[1])

    logging.info("Making barchart for inclusive losses")
    logging.debug("lumi_loss_data: %s" % (lumi_loss_data))
    make_barchart(os.path.join(directory, "InclusiveLosses.png"), lumi_loss_data)

    # here we start doing mixed lumi losses
    logging.info("Doing analysis for exclusive losses")
    # Calculating the loss for Pixel and Strip HV transition
    run_compareJSON(directory, "json_2.txt", "json_4.txt", "json_PixOff.txt")
    run_compareJSON(directory, "json_2.txt", "json_3.txt", "json_StripOff.txt")
    run_compareJSON(directory, "json_StripOff.txt", "json_PixOff.txt",
            "json_TRKOff.txt", "--and")

    calculate_exclusive_losses()
    calculate_mixed_losses()
    calculate_exclussie_loss_by_run()
    calculate_trk_HV_losses()

    if __first_run == 294927:
        logging.debug("Adding lumi lost for Comissioning")
        __exclusive_detector_losses["Commissioning"] = reclumilostComm
        with open((os.path.join(directory, "DataLoss.txt")), "a") as f2:
            f2.write("Commissioning %.1f\n" % (reclumilostComm))

    logging.debug("data_to_render: %s" % (data_to_render))
    logging.info("Number of bad detectors: %s" % (len(__exclusive_detector_losses)))
    logging.info("Making piechart for inclusive losses")
    make_pizzachart(os.path.join(directory, "ExclusiveLosses.png"),
            __exclusive_detector_losses)

    with open((os.path.join(directory, "index.html")), "w") as f:
        f.write(render("lumi_loss_tpl.html", data_to_render))