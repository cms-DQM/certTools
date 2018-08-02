#!/usr/bin/env python

import sys
import json
import argparse

from operator import itemgetter

from rrapi import RRApi, RRApiError
from rhapi import RhApi

def get_runs(query):
    """
    method to get list of runs for specified query
    """
    tmp_data = query_runregistry(query)
    # parse to useful format
    run_list = [el[0] for el in tmp_data]

    return run_list

def get_dataset_lumis(query, run_list, save=False):
    """
    get list of runs matching specified dataset and filters from online table
    """
    main_obj = {}
    tmp_data = query_runregistry(query)
    # in case for debugging
    if options.verbose:
        print(run_list)

    # convert to useful format
    for el in tmp_data:
        if el[0] not in run_list:
            print("Dataset run: %s was not in runlist" % (el[0]))
            continue
        
        if el[0] not in main_obj:
            main_obj[el[0]] = []
        
        main_obj[el[0]].append([el[2],el[3]])

    # sort inner lists of lumisection in ascending order
    for el in main_obj:
        main_obj[el] = sorted(main_obj[el], key=itemgetter(0))

    if options.verbose:
        print(main_obj)

    print("Total runs with lumisections: %s" % (len(main_obj)))

    if bool(save):
        with open(save, "w") as f:
            f.write(json.dumps(main_obj, indent=2, sort_keys=True))

    return main_obj

def query_runregistry(query):
    """
    query run registry's tables using resthub API
    """
    try:
        api = RhApi("http://vocms00170:2113", debug=True)
        rr_data = api.json_all(query, inline_clobs=True)
    except Exception as ex:
        print("Error while using RestHub API: %s" % (ex))
        sys.exit(-1)

    return rr_data


def construct_dataset_query(minRun, group='Collisions18'):
    """
    construct query to join online dataset_lumis table with 
    dataset_offline table which has class_name etc.
    """

    params = []
    query = ("select d.RUN_NUMBER,d.RDA_NAME, dl.RDR_SECTION_FROM, dl.RDR_SECTION_TO from runreg_global.dataset_lumis_off dl, runreg_global.datasets_off d where "
    " d.RUN_CLASS_NAME = '%s' AND d.RDA_NAME like '%%Online%%ALL' and d.RDA_ONLINE = 1 AND dl.RDR_RUN_NUMBER = d.RUN_NUMBER AND dl.RDR_RDA_NAME = d.RDA_NAME" % (group))    

    params.append("nvl(dl.FPIX_READY, 1) = 1")
    params.append("nvl(dl.BPIX_READY, 1) = 1")
    params.append("nvl(dl.TECM_READY, 1) = 1")
    params.append("nvl(dl.TECP_READY, 1) = 1")
    params.append("nvl(dl.TOB_READY, 1) = 1")
    params.append("nvl(dl.TIBTID_READY, 1) = 1")
    params.append("nvl(dl.CMS_ACTIVE, 1) = 1")
    params.append("nvl(dl.BEAM1_PRESENT, 1) = 1")
    params.append("nvl(dl.BEAM2_PRESENT, 1) = 1")
    params.append("nvl(dl.BEAM1_STABLE, 1) = 1")
    params.append("nvl(dl.BEAM2_STABLE, 1) = 1")

    return query + " AND " + " AND ".join(params)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description='Give list of Collisions runs for Online datasets')
    parser.add_argument("-o", "--outfile",
        dest="outfile", type=str, default="", help="Output file name")
    parser.add_argument("-v", "--verbose",
            dest="verbose", action="store_true", default=False, help="Display more info")
    parser.add_argument("-g", "--group",
        dest="dataset_group", type=str, default="Collisions18", help="Run class type")

    options = parser.parse_args()

    run_query = ("select r.RUNNUMBER from runreg_global.runs_on r where "
     "r.RUNNUMBER >= 268000 AND r.PIXEL_PRESENT = 1 AND r.TRACKER_PRESENT = 1")
    runslist = get_runs(run_query)
    if options.verbose:
        print(runslist)

    ds_query = construct_dataset_query(268000, group=options.dataset_group)
    res = get_dataset_lumis(ds_query, runslist, save=options.outfile)
    print("%s" % (json.dumps(res, sort_keys=True)))
