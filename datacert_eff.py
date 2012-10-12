#!/usr/bin/env python

""" Documentation.

The aim of this module is to make some further studies (w.r.t. the
ones done by lumiweb) to try to have a more precise picture of where
the lost luminosity has gone in CMS. In order to properly use it you
need to have available on disk the different JSON files for the
different scenarios produced by lumiweb: this script has no connection
with the RR. The different scenarios are then compared always in the
form of efficiencies using one scenario as numerator and the other one
as denominator. The efficiencies are computed as recorded_lumi of the
numerator scenario divided by the recorded luminosity of the
denominator scenario, both for every run and cumulatively, the weight
being the total recorded luminosity. A final ROOT file is written to
disk. The ROOT file contains a folder for every comparison and, within
this folder, a subfolder corresponding to the quantity with respect to
which you are computing the running efficiency, e.g.

Scenario_A_vs_Scenario_B/Vs_{Run,IntLumi,Lumi}

In order to avoid holes in the case in which a specific run is missing
from one scenario, the IntLumi is precomputed using the most inclusive
JSON file, currently the NoDCS and NoDQM one.

1. How to Use it

   These are quick instructions that you need to follow in order to
   properly run the script.

   1.1 Pre-Requisite

       1. A local CMSSW installation
       2. You need to have the json scenarios that are the output of
          the lumiweb script in the local directory that you will use
          to save the results.

       3. You first need to update the local disk-cache that holds all
          the luminosity information that is used to produce the
          results. You can do that in 2 ways that will produce
          different results.

          Method 1

          This method will locally cache the information coming only
          from the runs contained in the specified JSON file, which
          usually must be the most inclusive available,
          i.e. DCS-Only. This has potentially the problem that the
          total reported delivered and recorded luminosity may not
          match with the official one daily updated at
          https://twiki.cern.ch/twiki/bin/view/CMSPublic/LumiPublicResults. To
          overcome the problem and have a full information on all
          possible delivered and recorded luminosity, independent from
          the run list available from RR, one must use the Method2.
          
          > python
          python>import datacert_eff as dc
          python>dc.update_disk_cache_from_file('json_2.txt')

          Method 2

          This method will accept a run range and, within the range,
          will first query the lumiCalc2 script to have the full
          overview of teh period. Then, for each and every run listed
          in the main overview, the details are locally cache. This
          method is fully independent from any information store in
          RR3.

          > python
          python>import datacert_eff as dc
          python>dc.update_disk_cache_from_range(190389, 203002)

   1.2 Standalone mode

       Once all the pre-requisites are met, you can simply run the
       script as:
       
       python datacert_eff.py

       This will produce the Efficiencies.root file, together with one
       PDF file, one for each detector, showing 3 efficiencies: the
       DCS efficiency w.r.t the total recorded luminosity, the
       DataCertification Efficiency w.r.t. good DCS data for the
       specific detector and the global efficiency, which is merely
       the product of the previous two. This allow to disentangle
       where the luminosity is lost in CMS, in which subdetector and
       in which process: DCS or DataQuality.

   1.3 Use as a module

       This file can be used also as a module in case you want to run
       only part of the overall workflow.

"""

import cjson
import os
import re
import sys
from threading import Thread, activeCount
from time import sleep
from ROOT import TFile, TGraph, TH1F, gDirectory, gROOT, TCanvas, kRed, kBlue, TLegend
from array import array
from hashlib import sha256


class LumiDiskCacheUpdater(Thread):
    """ Single Thread class responsible for locally caching the
    luminosity information of a single run.  The cache is organized in
    a hierarchical set of folders, one per run. Each folder contains a
    single file with all the relevant information, as explained in the
    run method documentation. This partition allows immediate access
    to the luminosity information of a specific run simply by opening
    the proper file. Given the current size of all the cache (tens of
    MB) we can think of holding all the cache in memory for even
    faster access."""

    # Local _relative_ folder that will hold the luminosity cache.
    LUMI_CACHE_FOLDER_ = 'lumi_disk_cache'
    
    def __init__(self, run_number):
        Thread.__init__(self)
        self._run = run_number

    def run(self):
        """The local cache will hold only results related to the
        stable beam condition: all other beam flags are silently
        ignored and will not be included in the cache. The first line
        in the cache file is the result of the overview query, while
        all other lines are the outcome of the lumibyls flag. This
        speeds up the retrival of the overview information, since only
        the first cache line has to be read.  """
        
        directory = '%s/%s/' % (LumiDiskCacheUpdater.LUMI_CACHE_FOLDER_,
                                self._run)
        if not os.path.exists(directory):
            os.makedirs(directory)
        pipe = os.popen('lumiCalc2.py -r %s -b stable overview -o stdout' % self._run)
        w = open('%s/%s.txt' % (directory, self._run), 'w')
        w.write(pipe.readlines()[-1])
        pipe = os.popen('lumiCalc2.py -r %s -b stable lumibyls -o stdout' % self._run)
        for line in pipe.readlines():
            if (re.match("^\['", line)):
                w.write(line)


def _update_disk_cache(run_list):
    """Read an input list @run_list and locally saves a cached copy of
    the luminosity for each run. """

    NUM_THREADS = 40
    for run in run_list:
        while (1):
            print activeCount()
            if activeCount() <= NUM_THREADS:
                run_thread = LumiDiskCacheUpdater(run)
                run_thread.start()
                break
            else:
                sleep(1)


def update_disk_cache_from_file(file):
    """Read an input file @file in json format and retrieves the
    luminosity information for all the runs included in the JSON
    file. This method should be called interactively from a python
    shell, after having included this module.  """

    if not os.path.exists(file):
        return
    f = open(file, 'r')
    cert = eval([''.join(line) for line in f.readlines()][0])
    _update_disk_cache(sorted(cert.keys()))


def update_disk_cache_from_range(run_begin, run_end):
    
    """Update the local disk cache fetching all run registered in the
    lumiCalc2 database.

    For performance reasons it is much faster to first ask to
    lumiCalc2 the overall overview of all the known runs in stable
    beam conditions and only after gather all the information relative
    to each of them. The approach of querying lumiCalc2 for each
    andevery run from run_begin to run_end is much much slower, since
    the fractions of the runs in stable beam with respect to the total
    is minimal. """

    command = "lumiCalc2.py --begin=%d --end=%d --amodetag=PROTPHYS -b stable -o stdout overview" % (run_begin, run_end)
    run_list = []
    pipe = os.popen(command)
    for l in pipe.readlines():
        if re.match("^\*", l):
            continue
        if re.match("^\[WARNING\]", l):
            continue
        if re.match('^Run:Fill.*', l):
            continue
        run_list.append(l.strip().split(',')[0].strip("[").strip("'").split(':')[0])
    _update_disk_cache(run_list)


def get_hash(ls_ranges):
    """ Generate a hash key based on the LS intervals.  """

    to_hash = ''
    for ranges in ls_ranges:
        to_hash += '%s%s' % (ranges[0], ranges[-1])
    return sha256(to_hash).hexdigest()


def get_luminosity_cached_from_memory(run, ls_ranges, memory_cache, certified=False):
    """ Extract luminosity information from memory, if it is
    available. Each missed cache is internally redirected to the file
    based approach.  """

    if memory_cache.get(run):
        if not certified:
            if memory_cache[run].get('total'):
                return memory_cache[run]['total']
            else:
                return get_luminosity_cached(run, ls_ranges, memory_cache, certified)
        else:
            key_ls = get_hash(ls_ranges)
            if memory_cache[run].get(key_ls):
                return memory_cache[run][key_ls]
            return get_luminosity_cached(run, ls_ranges, memory_cache, certified)
    else:
        return get_luminosity_cached(run, ls_ranges, memory_cache, certified)


def get_luminosity_cached(run, ls_ranges, memory_cache, certified=False):
    """Read luminosity information for the specified run @run and lumi
    section range @ls_range directly from the local cache. If
    @certified is False the overview result is returned, read from the
    first line in the cache file. If @certified is True, the sum of
    the luminosity in the lumi section range @ls_ranges is
    returned.Returns a tuple structure as (delivered, recorded), in
    inverse picobarns. (0., 0.) is returned in case of problems
    reading the overview information. A warning message is issued
    everytime the luminosity information is missing for a specific
    lumisection."""

    directory = 'lumi_disk_cache/%s/' % run
    if os.path.exists(directory):
        lumi_file = open('%s/%s.txt' % (directory, run), 'r')
        lumi_line = []
        if not certified:
            lumi_line = lumi_file.readline()
            if re.match('^\[INFO', lumi_line):
                return (0., 0.)
            lumi_line = lumi_line.split(',')
            try:
                memory_cache.setdefault(run, {}).setdefault('total', (float(lumi_line[2].strip().strip("]"))/10.**6, float(lumi_line[-1].strip().strip("]"))/10.**6))
                return (float(lumi_line[2].strip().strip("]"))/10.**6, float(lumi_line[-1].strip().strip("]"))/10.**6)
            except:
                print 'ERROR: something wrong in run %s:\n %s' % (run, lumi_line)
                return(0., 0.)
        else:
            lumi_byls = {}
            delivered_lumi_certified = 0
            recorded_lumi_certified = 0
            # read overvierw information first.
            lumi_file.readline()
            for line in lumi_file.readlines():
                lumi_values = line.strip(']').strip('[').replace("'", '').split(',')
                lumi_section = int(lumi_values[1].split(':')[0])
                lumi_byls[lumi_section] = (float(lumi_values[5])/10.**6,
                                           float(lumi_values[6])/10.**6)
            for ranges in ls_ranges:
                for lumi in range(ranges[0], ranges[-1]+1):
                    try:
                        delivered_lumi_certified += lumi_byls[lumi][0]
                        recorded_lumi_certified += lumi_byls[lumi][1]
                    except:
                        pass
            memory_cache.setdefault(run, {}).setdefault(get_hash(ls_ranges),
                                                        (delivered_lumi_certified, recorded_lumi_certified))
            return (delivered_lumi_certified, recorded_lumi_certified)
    else:
        return(0., 0.)


def get_luminosity_overview(command):
    """Fetch luminosity information from the DB.

    Retrieves proper luminosity information from the LumiDB, as
    requested by the user.

    Args:
        command: the command line that needs to be execute to retireve
            information from the DB. It only works in overview mode since we
            always extract the information from the last line of command's
            output.
    Returns:
        A tuple with the following float luminosity values in pb-1:

        (delivered_luminosity, recorder_luminosity)
    """

    pipe = os.popen(command)
    lumi_line = eval(pipe.readlines()[-1])
    return (float(lumi_line[2])/10.**6, float(lumi_line[4])/10.**6)


def compute_efficiencies(denominator_lumi, numerator_lumi, weight, weighted_mean_numerator, sum_weights):
    """Compute efficiencies based on the input parameters. Returns a
    tuple consisting of (efficiency, weighted efficiency,
    weighted_mean_numerator, sum_weights). We return the partial
    numerator and denominator of the weighed mean calculation to ease
    the calculation of weighted values iteratively. It is
    responsibility of the calling function to properly store these
    numbers and keep on passing them to this routine.  """

    if denominator_lumi == 0:
        return (-1, -1,
                weighted_mean_numerator,
                sum_weights)
    eff  = numerator_lumi/denominator_lumi
    sum_weights += weight
    weighted_mean_numerator += (eff * weight)
    if sum_weights == 0:
        weighted_mean = 0.
    else:
        weighted_mean = weighted_mean_numerator / sum_weights
    return (eff, weighted_mean,
            weighted_mean_numerator,
            sum_weights)

def ROOTsave(x_axis, y_axes):
    """ROOT saving function.  """

    N_BINS_PROJ = 100
    x_axis_arr = array('d')
    x_axis_arr.fromlist(x_axis)
    for y_axis in y_axes:
        if not y_axis.get('values'):
            continue
        y_axis_arr = array('d')
        y_axis_arr.fromlist(y_axis['values'])
        tg = TGraph(len(x_axis_arr), x_axis_arr, y_axis_arr)
        tg.SetTitle("%s" % y_axis['y_label'])
        tg.Write("%s" % y_axis['y_label'])
        if y_axis.get('y_projection'):
            th1 = TH1F("%s_projection" % y_axis['y_label'],
                       "%s_projection" % y_axis['y_label'],
                       N_BINS_PROJ, y_axis['y_range'][0],
                       y_axis['y_range'][1])
            for value in y_axis['values']:
                if value > 0:
                    th1.Fill(float(value))
            th1.Write()


def compute_int_lumi(file_baseline, int_lumi, memory_cache):
    """Pre-compute the incremental delivered and recorder luminosity
    for a given scenario for ultra-fast access to it. This also avoids
    the rendering problems due to possible missing runs in some
    scenario: the incremantal luminosity _must_ be computed on the
    largest available scenario. If we use the DCS json we inevitably
    miss some runs, due to the requirements of the DCS only
    scenario. This is way we loop over all runs, from the minimum up
    to the maximum with step 1 and collect all luminosity
    information. This procedure requires that the user had updated the
    local cache using update_disk_cache_from_range, so that all runs
    are covered by the local cache.  Failing to run the proper
    cache-update function will bring the user back to the old, DCS
    only scenario, with all the losses."""

    full_file = open(file_baseline, 'r')
    full_file_content = [''.join(l) for l in full_file.readlines()]
    full_object = cjson.decode(full_file_content[0])
    all_del_tot_lumi = 0
    all_rec_tot_lumi = 0
    run_list = sorted(full_object.keys())
    for run in range(int(run_list[0]), int(run_list[-1])+1):
        obj = full_object.get(str(run), [[1-9999]])
        (del_tot_lumi, rec_tot_lumi) = get_luminosity_cached_from_memory(run,
                                                                         obj,
                                                                         memory_cache,
                                                                         False)
        all_del_tot_lumi += del_tot_lumi
        all_rec_tot_lumi += rec_tot_lumi
        int_lumi.setdefault(str(run), (all_del_tot_lumi, all_rec_tot_lumi))
    print 'Total delivered and recorded luminosity: (%d, %d)' % (all_del_tot_lumi, all_rec_tot_lumi)

def analyse_scenario(file_baseline, file_selection, ROOT_file, scenario_names, memory_cache, int_lumi):
    """Analyse a specific scenario. We receive in input two json
    files, @file_baseline and @file_selection, and perform a standard
    set of plot of information extracted from @file_selection versus
    information extracted from @file_baseline.  """

    gDirectory.cd('/')

    folder = '%s_vs_%s' % (scenario_names[int(re.match('json_(\d+).*', os.path.splitext(file_baseline)[0]).group(1))],
                           scenario_names[int(re.match('json_(\d+).*', os.path.splitext(file_selection)[0]).group(1))])
    gDirectory.mkdir(folder)
    gDirectory.cd(folder)
    full_file = open(file_baseline, 'r')
    full_file_content = [''.join(l) for l in full_file.readlines()]
    full_object = cjson.decode(full_file_content[0])
    selection_file = open(file_selection, 'r')
    selection_file_content = [''.join(l) for l in selection_file.readlines()]
    selection_object = cjson.decode(selection_file_content[0])
    sum_inv_tot_w = 0
    w_eff_rec_tot_over_del_tot = 0
    w_eff_rec_tot_over_del_tot_numerator = 0
    sum_inv_selection_w = 0
    w_eff_rec_selection_over_rec_tot = 0
    w_eff_rec_selection_over_rec_tot_numerator = 0
    all_del_selection_lumi = 0
    all_rec_selection_lumi = 0
    run_list = []
    list_del_tot_lumi = []
    list_del_lumi = []
    list_eff_rec_tot_over_del_tot = []
    list_w_eff_rec_tot_over_del_tot = []
    list_eff_rec_selection_over_rec_tot = []
    list_w_eff_rec_selection_over_rec_tot = []
    run_count = 0

    print "\n%s vs %s" % (file_baseline, file_selection)
    for run in sorted(full_object.keys()):
        run_count += 1
        if run_count%25 == 0:
            sys.stdout.write('.')
            sys.stdout.flush()

        run_list.append(int(run))
        (del_tot_lumi, rec_tot_lumi) = get_luminosity_cached_from_memory(run,
                                                                         full_object[run],
                                                                         memory_cache,
                                                                         certified=True)
        (eff_rec_tot_over_del_tot, w_eff_rec_tot_over_del_tot,
         w_mean_numerator, sum_w) = compute_efficiencies(del_tot_lumi,
                                                         rec_tot_lumi,
                                                         rec_tot_lumi,
                                                         w_eff_rec_tot_over_del_tot_numerator,
                                                         sum_inv_tot_w)
        w_eff_rec_tot_over_del_tot_numerator = w_mean_numerator
        sum_inv_tot_w = sum_w
        list_eff_rec_tot_over_del_tot.append(eff_rec_tot_over_del_tot)
        list_w_eff_rec_tot_over_del_tot.append(w_eff_rec_tot_over_del_tot)
        list_del_tot_lumi.append(int_lumi[run][0])
        list_del_lumi.append(del_tot_lumi)
        if run in selection_object.keys():
            (del_selection_lumi, rec_selection_lumi) = get_luminosity_cached_from_memory(run,
                                                                                         selection_object[run],
                                                                                         memory_cache,
                                                                                         certified=True)
        else:
            (del_selection_lumi, rec_selection_lumi) = (0., 0.)
        all_del_selection_lumi += del_selection_lumi
        all_rec_selection_lumi += rec_selection_lumi
        (eff_rec_selection_over_rec_tot, w_eff_rec_selection_over_rec_tot,
         w_mean_numerator, sum_w) = compute_efficiencies(rec_tot_lumi,
                                                         rec_selection_lumi,
                                                         rec_tot_lumi,
                                                         w_eff_rec_selection_over_rec_tot_numerator,
                                                         sum_inv_selection_w)
        w_eff_rec_selection_over_rec_tot_numerator = w_mean_numerator
        sum_inv_selection_w = sum_w
        list_eff_rec_selection_over_rec_tot.append(eff_rec_selection_over_rec_tot)
        list_w_eff_rec_selection_over_rec_tot.append(w_eff_rec_selection_over_rec_tot)
    x_axis_list = [(run_list, 'Vs_Run'),
                   (list_del_tot_lumi, 'Vs_IntLumi'),
                   (list_del_lumi, 'Vs_Lumi')]
    for x_type in x_axis_list:
        gDirectory.cd('/%s' % folder)
        x_folder = '%s' % x_type[1]
        gDirectory.mkdir('%s' % x_folder)
        gDirectory.cd('%s' % x_folder)
        ROOTsave(x_type[0], [{'values': list_eff_rec_tot_over_del_tot,
                              'y_label': 'rec_tot_vs_del_tot',
                              'y_range': [0., 1.05],
                              'y_projection': True},
                             {'values': list_eff_rec_selection_over_rec_tot,
                              'y_label': 'rec_selection_vs_rec_tot',
                              'y_range': [0., 1.05],
                              'y_projection': True},
                             {'values': list_w_eff_rec_tot_over_del_tot,
                              'y_label': 'running_wm_rec_tot_vs_del_tot',
                              'y_range': [0., 1.05],
                              'y_projection': True},
                             {'values': list_w_eff_rec_selection_over_rec_tot,
                              'y_label': 'running_wm_rec_selection_vs_rec_tot',
                              'y_range': [0., 1.05],
                              'y_projection': True},
                             ])


def makePDFs(root_file, subsystem, x_folder):
    print "Analyzing %s vs %s" % (subsystem, x_folder)
    what = "running_wm_rec_selection_vs_rec_tot"
    cert = root_file.Get("DQM_None_DCS_%s_vs_DQM_%s_DCS_%s/%s/%s" % (subsystem,
                                                                     subsystem,
                                                                     subsystem,
                                                                     x_folder,
                                                                     what))
    dcs = root_file.Get("DQM_None_DCS_None_vs_DQM_None_DCS_%s/%s/%s" % (subsystem,
                                                                        x_folder,
                                                                        what))
    total = root_file.Get("DQM_None_DCS_None_vs_DQM_%s_DCS_%s/%s/%s" % (subsystem,
                                                                        subsystem,
                                                                        x_folder,
                                                                        what))
    d = TCanvas(subsystem, subsystem, 800, 800)
    cert.SetMinimum(0.95)
    cert.SetMaximum(1.01)
    cert.Draw("AP")
    dcs.SetMarkerColor(kRed)
    dcs.Draw("P")
    total.SetMarkerColor(kBlue)
    total.Draw("P")
    leg = TLegend(0.6, 0.8, 0.9, 0.9);
    leg.SetTextFont(102)
    leg.SetHeader("Info");
    leg.AddEntry(cert,  "Cert_Eff([DQM+DCS]/[DCS])", "lp");
    leg.AddEntry(dcs,   "DCS_Eff([DCS]/[ALL])", "lp");
    leg.AddEntry(total, "Total_Eff([DQM+DCS]/[ALL])", "lp");
    leg.Draw();
    d.SaveAs("%s_%s.pdf" % (subsystem, x_folder))


def main():
    gROOT.SetBatch(True)
    scenario_names = ['DQM_All_DCS_All',     # 0
                      'DQM_None_DCS_All',    # 1
                      'DQM_None_DCS_None',   # 2
                      'DQM_None_DCS_Strip',  # 3
                      'DQM_None_DCS_Pix',    # 4
                      'DQM_None_DCS_Ecal',   # 5
                      'DQM_None_DCS_Es',     # 6
                      'DQM_None_DCS_Hcal',   # 7
                      'DQM_None_DCS_Dt',     # 8
                      'DQM_None_DCS_Csc',    # 9
                      'DQM_None_DCS_Rpc',    # 10
                      'DQM_Muon_DCS_None',   # 11
                      'DQM_Jetmet_DCS_None', # 12
                      'DQM_Egamma_DCS_None', # 13
                      'DQM_Track_DCS_None',  # 14
                      'DQM_Rpc_DCS_Rpc',     # 15
                      'DQM_Csc_DCS_Csc',     # 16
                      'DQM_Dt_DCS_Dt',       # 17
                      'DQM_Hcal_DCS_Hcal',   # 18
                      'DQM_Ecal_DCS_Ecal',   # 19
                      'DQM_Es_DCS_Es',       # 20
                      'DQM_Strip_DCS_Strip', # 21
                      'DQM_Pix_DCS_Pix',     # 22
                      'DQM_Hlt_DCS_None',    # 23
                      'DQM_L1t_DCS_None',    # 24
                      'DQM_Lumi_DCS_None',   # 25
                      'DQM_etrk_DCS_etrk',   # 26
                      'DQM_caltrk_DCS_caltrk',       # 27
                      'DQM_muonphys_DCS_muon phys',  # 28
                      'DQM_9ONbutHlt_DCS_default',   # 29
                      'DQM_9ONbutL1t_DCS_default',   # 30
                      'DQM_9ONbutPix_DCS_default',   # 31
                      'DQM_9ONbutStrip_DCS_default', # 32
                      'DQM_9ONbutEcal_DCS_default',  # 33
                      'DQM_9ONbutEs_DCS_default',    # 34
                      'DQM_9ONbutHcal_DCS_default',  # 35
                      'DQM_9ONbutDt_DCS_default',    # 36
                      'DQM_9ONbutCSC_DCS_default',   # 37
                      'DQM_9ONbutRPC_DCS_default',   # 38
                      ]
    memory_cache = {}
    int_lumi = {}
    compute_int_lumi("json_2.txt", int_lumi, memory_cache)
    ROOT_filename = 'Efficiencies.root'
    ROOT_file = TFile(ROOT_filename, 'RECREATE')
    matrix_comparison = [(7, 18), (5, 19), (6, 20), (4, 22), (3, 21), (8, 17), (9, 16), (10, 15)]
    for target_json in range (0,39):
        analyse_scenario('json_2.txt',
                         'json_%d.txt' % target_json,
                         ROOT_file,
                         scenario_names,
                         memory_cache,
                         int_lumi)
    for (den, num) in matrix_comparison:
        analyse_scenario('json_%d.txt' % den,
                         'json_%d.txt' % num,
                         ROOT_file,
                         scenario_names,
                         memory_cache,
                         int_lumi)
    ROOT_file.Close()

    file = TFile(ROOT_filename, "READ")
    subsys = ['Strip', 'Pix', 'Ecal', 'Es', 'Hcal', 'Dt', 'Csc', 'Rpc']
    for s in subsys:
        makePDFs(file, s, 'Vs_Run')
        makePDFs(file, s, 'Vs_IntLumi')
        makePDFs(file, s, 'Vs_Lumi')

if __name__ == '__main__':
    main()
