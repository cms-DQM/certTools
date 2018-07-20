#!/usr/bin/env python

import sys
import json
import argparse

from rhapi import RhApi

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
            description='Give list of Collisions runs for Online datasets')
    parser.add_argument("-v", "--verbose",
            dest="verbose", action="store_true", default=False, help="Display more info")
    parser.add_argument("-m", "--min",
            dest="min", type=int, default=294000, help="Minimum run")
    parser.add_argument("-M", "--max",
            dest="max", type=int, default=999999, help="Maximum run")
    parser.add_argument("-i", "--infile",
        dest="infile", type=str, default="",help="Text file with run list")

    options = parser.parse_args()

    if options.verbose:
        print("Run selection: %s" % (runsel))

    __run_alias = "r"
    __dataset_alias = "d"
    __query = ("select %s.RUNNUMBER, %s.EVENTS, %s.BFIELD, %s.HLTKEYDESCRIPTION "
        "from runreg_global.runs_on %s, runreg_global.datasets_on %s where ") % (
            __run_alias, __run_alias, __run_alias,
            __run_alias, __run_alias, __dataset_alias)

    __query += "%s.RUNNUMBER >= %s AND %s.RUNNUMBER <= %s " % (__run_alias,
            options.min, __run_alias, options.max)

    if options.infile:
        print("Opening file %s which contains the run list" % (options.infile))
        with open(options.infile, "r") as inputfile:
            for run in inputfile:
                print(run)
                __query += "OR %s.RUNNUMBER = %s " % (__run_alias, run)

    __query += "AND %s.RUN_CLASS_NAME = '%s' " % (__run_alias, "Collisions18")
    __query += "AND %s.RDA_NAME like '%s' " % (__dataset_alias, "%Online%")

    # do a join
    __query += "AND %s.RUNNUMBER = %s.RUN_NUMBER" % (__run_alias, __dataset_alias)

    try:
        api = RhApi("http://vocms00170:2113", debug=True)
        rr_data = api.json(__query, inline_clobs=True)
    except Exception as ex:
        print("Error while using RestHub API: %s" % (ex))
        sys.exit(-1)

    # print rr_data
    print("RUN_NUMBER\tEVENTS\tBFIELD\tHLTKEYDESCRIPTION")
    rr_data["data"].sort(key=lambda x: x[0])
    for el in rr_data["data"]:
        if el[1] != None:
            print("%s\t%s\t%s\t%s" % (el[0], el[1], el[2], el[3]))
        else:
            print("%s\t\t%s\t%s" % (el[0], el[2], el[3]))
