#! /usr/bin/env python

import sys
import json
import commands
import ConfigParser

from rhapi import RhApi

class Certifier():

    cfg = 'runreg.cfg'
    OnlineRX = "%Online%ALL"
    EXCL_LS_BITS = ('jetmet','muon','egamma')
    EXCL_RUN_BITS = ('all')

    def __init__(self, argv, verbose=False):
        self.verbose = verbose
        if len(argv) == 2:
            self.cfg = argv[1]
        else:
            self.cfg = Certifier.cfg

        self.restHubURL = "http://vocms00170:2113"
        self.qry = {}
        self.qry.setdefault("GOOD", "isNull OR = true")
        self.qry.setdefault("BAD", "0")
        self.readConfig()

    def readConfig(self):
        CONFIG = ConfigParser.ConfigParser()
        if self.verbose:
            print('Reading configuration file from %s') % (self.cfg)

        CONFIG.read(self.cfg)
        cfglist = CONFIG.items('Common')
        self.dataset = CONFIG.get('Common','DATASET')
        self.group   = CONFIG.get('Common','GROUP')
        self.address = CONFIG.get('Common','RUNREG')
        self.runmin  = CONFIG.get('Common','RUNMIN')
        self.runmax  = CONFIG.get('Common','RUNMAX')
        self.runlist = []

        for item in cfglist:
            if "RUNLIST" in item[0].upper():
                for el in item[1].split(" "):
                    # in case somebody put single quotes
                    el.replace("'", '"')
                    if el.startswith('"'):
                        el = el[1:]
                    if el.endswith('"'):
                        el = el[:-1]
                    self.runlist.append(int(el))

        self.qflist  = CONFIG.get('Common','QFLAGS').split(',')
        self.bfield_thr  = '-0.1'
        self.bfield_min  = '-0.1'
        self.bfield_max  = '4.1'
        self.injection  = "%"
        self.dcslist = CONFIG.get('Common','DCS').split(',')
        self.jsonfile = CONFIG.get('Common','JSONFILE')
        self.beamene     = []
        self.dbs_pds_all = ""
        self.online_cfg  = "FALSE"
        self.usedbs = False
        self.useDAS = False
        self.dsstate = ""
        self.useDBScache = "False"
        self.useBeamPresent = "False"
        self.useBeamStable = "False"
        self.cacheFiles = []
        self.predefinedPD = ["/Commissioning/Run2015A-v1/RAW",
                "/ZeroBias/Run2015B-v1/RAW", "/ZeroBias/Run2016B-v1/RAW"]

        self.component = []
        self.nolowpu = "True"
        self.use_offline_DB = "True"

        print("First run: %s" % (self.runmin))
        print("Last run: %s" % (self.runmax))
        if len(self.runlist) > 0:
            print("List of %s runs in runlist: %s" % (len(self.runlist), self.runlist))

        print("Dataset name: %s" % (self.dataset))
        print("Group name: %s" % (self.group))
        print("Quality flags: %s" % (self.qflist))
        print("DCS flags: %s" % (self.dcslist))

        for item in cfglist:
            if "INJECTION" in item[0].upper():
                self.injection  = item[1]
            if "BFIELD_THR" in item[0].upper():
                self.bfield_thr = item[1]
            if "BFIELD_MIN" in item[0].upper():
                self.bfield_min = item[1]
            if "BFIELD_MAX" in item[0].upper():
                self.bfield_max = item[1]
            if "BEAM_ENE" in item[0].upper():
                self.beamene = item[1].split(',')
            if "DBS_PDS" in item[0].upper():
                self.dbs_pds_all = item[1]
                self.usedbs = True
            if "USE_DAS" in item[0].upper():
                self.useDAS = item[1]
            if "ONLINE" in item[0].upper():
                self.online_cfg = item[1]
            if "DSSTATE" in item[0].upper():
                self.dsstate = item[1]
            if "DBSCACHE" in item[0].upper():
                self.useDBScache = item[1]
            if "BEAMPRESENT" in item[0].upper():
                self.useBeamPresent = item[1]
                print('Use Beam Present Flag: %s' % (self.useBeamPresent))
            if "BEAMSTABLE" in item[0].upper():
                self.useBeamStable = item[1]
                print('Use Beam Stable Flag: %s' % (self.useBeamStable))
            if "CACHEFILE" in item[0].upper():
                self.cacheFiles = item[1].split(',')
            if "COMPONENT" in item[0].upper():
                self.component = item[1].split(',')
                print('COMPONENT: %s' % (self.component))
            if "NOLOWPU" in item[0].upper():
                self.nolowpu = item[1]
                print('NoLowPU: %s' % (self.nolowpu))
            if "OFFLINEDB" in item[0].upper():
                self.use_offline_DB = item[1]
                print('Using online tables: %s' % (self.use_offline_DB))

        self.dbs_pds = self.dbs_pds_all.split(",")

        print("Injection schema: %s" % (self.injection))

        if self.useDAS == "True":
            self.usedbs = False

        print("Using DAS database: %s"  % (self.useDAS))
        print("Using Cache: %s" % (self.useDBScache))

        self.online = False
        if ("TRUE" == self.online_cfg.upper() or
               "1" == self.online_cfg.upper() or "YES" == self.online_cfg.upper()):
            self.online = True

        try:
            self.bfield_min = float(self.bfield_min)
        except:
            print("Minimum BFIELD value not understood: %s" % (self.bfield_min))
            sys.exit(1)

        try:
            self.bfield_max = float(self.bfield_max)
        except:
            print("Maximum BFIELD value not understood: %s" % (self.bfield_max))
            sys.exit(1)

        try:
            self.bfield_thr = float(self.bfield_thr)
        except:
            print("Threshold BFIELD value not understood: %s" %(self.bfield_thr))
            sys.exit(1)

        if self.bfield_thr > self.bfield_min:
            self.bfield_min = self.bfield_thr

        for e in range(0, len(self.beamene)):
            try:
                self.beamene[e] = float(self.beamene[e])
                if self.verbose:
                    print("Beam Energy: %s" % (self.beamene))
            except:
                print("BEAMENE value not understood: %s" % (self.beamene))
                sys.exit(1)

    def generate_runs_query(self):
        """
        make query for rr4 api to return list of runs from runs_offline DB
        """

        table_name = "r"
        params = []
        if self.use_offline_DB == "True":
            db_name = "runs_off"
        else:
            db_name = "runs_on"

        __query = "select %s.RUNNUMBER from runreg_global.%s %s where " % (table_name,
                db_name, table_name)

        # run query
        params.append("(%s.BFIELD > %.1f AND %s.BFIELD < %.1f)" % (table_name,
                self.bfield_min, table_name, self.bfield_max))

        if self.group.startswith("Collisions"):
            params.append("%s.INJECTIONSCHEME like '%s'" % (table_name, self.injection))

        for comp in self.component:
             if comp != 'NONE':
                params.append("%s.%s_PRESENT = 1" % (table_name, comp.upper()))

        if len(self.beamene):
            eneQuery = '(%s.LHCENERGY IS NULL OR %s.LHCENERGY = 0 )' % (table_name, table_name)
            for e in self.beamene:
                energyLow = e - 400
                if energyLow < 0:
                    energyLow = 0
                energyHigh = e + 400
                eneQuery = eneQuery[:-1] # remove last bracket of previous query
                eneQuery += 'OR (%s.LHCENERGY >= %.1d AND %s.LHCENERGY <= %.1d) )' % (table_name,
                        energyLow, table_name, energyHigh)

            params.append(eneQuery)

        params.append("%s.RUNNUMBER >= %s AND %s.RUNNUMBER <= %s" % (table_name,
                self.runmin, table_name, self.runmax))

        return __query + " AND ".join(params)

    def get_list_of_runs(self, query):
        """
        return list of runs from restHub API
        """

        try:
            api = RhApi(self.restHubURL, debug=self.verbose)
            rr_data = api.json(query, inline_clobs=True)
        except Exception as ex:
            print("Error while using RestHub API: %s" % (ex))
            sys.exit(-1)

        # convert resthub format to useful list of runs
        # 0th value is runnumber as specified in __query

        list_of_runs = [el[0] for el in rr_data["data"]]
        return list_of_runs

    def get_list_of_lumis(self, query):
        """
        get list of lumis for runs with specified query
        """

        try:
            api = RhApi(self.restHubURL, debug=self.verbose)
            rr_data = api.json(query, inline_clobs=True)
        except Exception as ex:
            print("Error while using RestHub API: %s" % (ex))
            sys.exit(-1)

        return rr_data

    def generate_runs_of_lumis(self, data, run_list):
        """
        make a run: [[lumi1,lumi2],[lumi4,lumiN]] structure for specified data
        check if run fom data is in run_list
        data[0] is run_number
        data[1] is section_from
        data[2] is section_to
        """
        __actual_data = {}
        for el in data:
            if el[0] not in run_list:
                print("run: %s in lumi_table not in run_list" % (el[0]))
                continue
            if el[0] in self.runlist:
                print("run: %s is specified in config RUNLIST" % (el[0]))
                continue
            #if run not in data we create an empty list and append section
            if el[0] not in __actual_data:
                __actual_data[el[0]] = []
                __actual_data[el[0]].append([el[1],el[2]])
            else:
                __actual_data[el[0]].append([el[1],el[2]])

        #we have to sort list of sections for each for:
        for el in __actual_data:
            __actual_data[el].sort()
            __actual_data[el] = merge_intervals2(__actual_data[el])

        return __actual_data

    def generateFilter(self):
        """
        generate resthub query for dataset_offline table and dataset_lumis_off table
        doing a join on dataset names
        """
        lumi_table = "dl"
        dataset_table = "d"
        if self.use_offline_DB == "True":
            lumi_table_name = "dataset_lumis_off"
            dataset_table_name = "datasets_off"
        else:
            lumi_table_name = "dataset_lumis_on"
            dataset_table_name = "datasets_on"

        __query = ("select %s.RDR_RUN_NUMBER, %s.RDR_SECTION_FROM, %s.RDR_SECTION_TO "
                "from runreg_global.%s %s, runreg_global.%s %s where ") % (
                lumi_table, lumi_table, lumi_table, lumi_table_name,
                lumi_table, dataset_table_name, dataset_table)

        params = []

        for qf in self.qflist:
            (key,value) = qf.split(':')
            if self.verbose:
                print("%s" % (qf))
            if key != "NONE":
                # Check if the bit is not excluded to avoide filter on LS for Egamma, Muon, JetMET
                if len([i for i in self.EXCL_LS_BITS if i == key.lower()]) == 0:
                    if value == "GOOD":
                        # if null default to true
                        params.append("nvl(%s.LSE_%s,1) = 1" % (lumi_table, key.upper()))
                    else:
                        # false
                        params.append("%s.LSE_%s = 0" % (lumi_table, key.upper()))

                # Check run flag
                if (self.EXCL_RUN_BITS != key.lower()):
                    # datasets_off query
                    params.append("%s.RDA_CMP_%s = '%s'" % (dataset_table, key.upper(), value))


        if self.nolowpu == "True":
            print("Removing low pile-up runs")
            params.append("nvl(%s.LSE_LOWLUMI, 0) = 0" % (lumi_table))
        else:
            print("Selecting ONLY low pile-up runs")
            params.append("%s.LSE_LOWLUMI = 1" % (lumi_table))

        for dcs in self.dcslist:
            if dcs != "NONE":
                if self.verbose:
                    print("%s" % (dcs))

                params.append("nvl(%s.%s_READY, 1) = 1" % (lumi_table, dcs.upper()))


        if self.useBeamPresent == "True":
            print("Removing LS with no beam present")
            params.append("nvl(%s.BEAM1_PRESENT, 1) = 1" % (lumi_table))
            params.append("nvl(%s.BEAM2_PRESENT, 1) = 1" % (lumi_table))

        if self.useBeamStable == "True":
            print("Removing LS with non-stable beams")
            params.append("nvl(%s.BEAM1_STABLE, 1) = 1" % (lumi_table))
            params.append("nvl(%s.BEAM2_STABLE, 1) = 1" % (lumi_table))

        params.append("%s.RDR_RUN_NUMBER >= %s" % (lumi_table, self.runmin))
        params.append("%s.RDR_RUN_NUMBER <= %s" % (lumi_table, self.runmax))
        params.append("nvl(%s.CMS_ACTIVE, 1) = 1" % (lumi_table))


        if self.online:
            # datasets_off query
            params.append("%s.RDA_NAME like '%s'" % (dataset_table, Certifier.OnlineRX))

            # WTF?? TO-DO: check which one it should look for online
            # self.filter.setdefault("dataset", {})\
            #                                   .setdefault("filter", {})\
            #                                   .setdefault("online", " = true")
        else:
            # datasets_off query
            datasetQuery = '()'
            for i in self.dataset.split():
                datasetQuery = datasetQuery[:-1]
                datasetQuery += " %s.RDA_NAME like '%s' OR)" % (dataset_table, i.split(":")[0])

            # remove the last OR) in query and replace it with bracket
            datasetQuery = datasetQuery[:-3] + ")"
            params.append(datasetQuery)

        # datasets_off query
        params.append("%s.RUN_CLASS_NAME = '%s'" % (dataset_table, self.group))

        if len(self.dsstate):
            # datasets_off query
            params.append("%s.RDA_STATE = '%s'" % (dataset_table, self.dsstate))

        # make a join of dataset table and lumi tables
        params.append("%s.RDR_RUN_NUMBER = %s.RUN_NUMBER" % (lumi_table, dataset_table))
        params.append("%s.RDR_RDA_NAME = %s.RDA_NAME" % (lumi_table, dataset_table))

        if self.verbose:
            print(__query + " AND ".join(params))

        return __query + " AND ".join(params)

    def filter_using_cmsweb(self):

        if self.verbose:
            print("Filtering JSON data:%s "  % (json.dumps(self.cert_json)))

        dbsjson = {}
        if self.useDBScache == "True":
            print("use cache")
            dbsjson = get_cachejson(self, self.dbs_pds_all)
        elif self.usedbs:
            dbsjson = get_dbsjson(self, self.dbs_pds_all, self.runmin, self.runmax, self.runlist)
        elif self.useDAS:
            dbsjson = get_dasjson(self, self.dbs_pds_all, self.runmin, self.runmax, self.runlist)
        else:
            # special case, e.g. cosmics which do not need DB or cache file
            print("INFO: no cache or DB option was selected in cfg file")

        if len(dbsjson) == 0:
            print("ERROR, dbsjson contains no runs, please check!")
            sys.exit(1)

        if self.verbose:
            print("Printing dbsjson %s" % (dbsjson))

        for element in self.cert_old_json:
            combined = []
            dbsbad_int = invert_intervals(self.cert_old_json[element])
            if self.verbose:
                print("debug: Good Lumi %s" % (self.cert_old_json[element]))
                print("debug: Bad Lumi %s" % (dbsbad_int))
            for interval in dbsbad_int:
                combined.append(interval)
            if element in dbsjson.keys():
                if self.verbose:
                    print("debug: Found in DBS Run: %s Lumi: %s" % (element, dbsjson[element]))

                dbsbad_int = invert_intervals(dbsjson[element])
                if self.verbose:
                    print("debug DBS Bad Lumi %s" % (dbsbad_int))
            else:
                dbsbad_int = [[1,9999]]
            for interval in dbsbad_int:
                combined.append(interval)
            combined = merge_intervals(combined)
            combined = invert_intervals(combined)
            if len(combined) != 0:
                self.cert_old_json[element] = combined

        if self.verbose:
            print("%s" % (json.dumps(self.cert_old_json)))

def invert_intervals(intervals, min_val=1, max_val=9999):
    if not intervals:
        return []

    intervals = merge_intervals(intervals)
    intervals = sorted(intervals, key = lambda x: x[0])
    result = []
    if min_val == -1:
        (a,b) = intervals[0]
        min_val = a
    if max_val == -1:
        (a,b) = intervals[len(intervals)-1]
        max_val = b

    curr_min = min_val
    for (x,y) in intervals:
        if x > curr_min:
            result.append((curr_min, x-1))
        curr_min = y + 1
    if curr_min < max_val:
        result.append((curr_min, max_val))

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

def merge_intervals2(intervals):
    """
    merge lumisections in case two ranges has same start & end lumisection
    """
    if not intervals:
        return []

    result = []
    (a, b) = intervals[0]
    for (x, y) in intervals[1:]:
        if x <= b + 1:
            b = max(b, y)
        else:
            result.append((a, b))
            (a, b) = (x, y)
    result.append((a, b))
    return result

def get_cachejson(self, datasets):
    unsorted = {}
    lumirangesjson = []
    lumirangejson = {}
    fileformatjson = False
    for ds in  datasets.split(","):
        if (ds in self.predefinedPD) :
            for cacheName in self.cacheFiles:
                cacheFile = open(cacheName)
                for line in cacheFile:
                    if '[' in line:
                        fileformatjson = True

                    runlumi = line.split()
                    if len(runlumi) > 1:
                        if runlumi[0].isdigit():
                            run = runlumi[0]

                            if fileformatjson:
                                runlumic = runlumi[1:]
                                lumirange = []
                                for i, v in enumerate(runlumic):
                                    if not i % 2:
                                        lowlumi = int(v.replace('[','').replace(']','').replace(',',''))
                                    else:
                                        highlumi = int(v.replace('[','').replace(']','').replace(',',''))
                                        lumi = range(lowlumi, highlumi + 1)
                                        lumirange += lumi
                                if run not in unsorted.keys():
                                    unsorted[run] = []
                                    unsorted[run] = lumirange
                            else:
                                if run not in unsorted.keys():
                                    unsorted[run] = []
                                    for lumi in runlumi[1:]:
                                        unsorted[run].append(int(lumi))
                cacheFile.close()

    sorted = {}
    for run in unsorted.keys():
       lumilist = unsorted[run]
       lumilist.sort()
       sorted[run] = lumilist

    dbsjson = {}
    for run in sorted.keys():
       lumilist = sorted[run]
       lumiranges = []
       lumirange = []
       lumirange.append(lumilist[0])
       lastlumi = lumilist[0]
       for lumi in lumilist[1:]:
           if lumi > lastlumi + 1:
               lumirange.append(lastlumi)
               lumiranges.append(lumirange)
               lumirange = []
               lumirange.append(lumi)
           lastlumi = lumi
       if len(lumirange) == 1:
           lumirange.append(lastlumi)
           lumiranges.append(lumirange)
       dbsjson[run] = lumiranges

    return dbsjson

def get_dbsjson(self, datasets, runmin, runmax, runlist):
    """
    DBS command line is deprecated
    """
    pass

def get_dasjson(self, datasets, runmin, runmax, runlist):
    unsorted = {}
    for ds in datasets.split(","):
        for runnm in range(int(runmin), int(runmax) + 1):
            foundrun = False
            if len(runlist) > 0:
                if runnm in runlist:
                    foundrun = True
            else:
                foundrun = True

            if foundrun:
                command = 'das_client.py --query="lumi,run dataset=%s run=%s system=dbs3" --format=json --das-headers --limit=0' % (ds, runnm)

                print(command)
                (status, out) = commands.getstatusoutput(command)
                if status:
                    sys.stderr.write(out)
                    print("ERROR on das command: %s\nHave you done cmsenv?" % (command))
                    sys.exit(1)

                js = json.loads(out)
                try:
                    js['data'][0]['run'][0]['run_number']
                except:
                    continue

                run = js['data'][0]['run'][0]['run_number']
                lumi = js['data'][0]['lumi'][0]['number']

                if run not in unsorted.keys():
                    unsorted[run] = []
                    for l in lumi:
                        unsorted[run].append(l)

            dasjson = {}
            for run in unsorted.keys():
                dasjson[str(run)]=unsorted[run]

    return dasjson

if __name__ == '__main__':
    cert = Certifier(sys.argv, verbose=True)

    rhub_query = cert.generateFilter()
    list_of_runs = cert.get_list_of_runs(cert.generate_runs_query())
    lumi_data = cert.get_list_of_lumis(rhub_query)
    cert.cert_json = cert.generate_runs_of_lumis(lumi_data["data"], list_of_runs)

    if (cert.useDBScache == "True" or
           cert.usedbs or cert.useDAS):

        cert.filter_using_cmsweb()

    with open(cert.jsonfile, "w") as f:
            json.dump(cert.cert_json, f, sort_keys=True)
            print("Json file: %s written.") % (cert.jsonfile)
