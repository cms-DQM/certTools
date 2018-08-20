import re
import sys
import array
import urllib2
import json

from x509auth import *
from ROOT import TBufferFile, TH1F, TProfile, TH1F, TH2F

def dqm_get_json(server, run, dataset, folder, rootContent=False, ident=None):
    datareq = urllib2.Request(('%s/data/json/archive/%s/%s/%s?rootcontent=1') % (
            server, run, dataset, folder))

    if ident:
        datareq.add_header('User-agent', ident)
    # Get data
    data = eval(re.sub(r"\bnan\b", "0", urllib2.build_opener(X509CertOpen()).open(datareq).read()),
               { "__builtins__": None }, {})

    if rootContent:
        # Now convert into real ROOT histograms
        for idx, item in enumerate(data['contents']):
            if 'obj' in item.keys():
                if 'rootobj' in item.keys():
                    a = array.array('B')
                    a.fromstring(item['rootobj'].decode('hex'))
                    t = TBufferFile(TBufferFile.kRead, len(a), a, False)
                    rootType = item['properties']['type']
                    if rootType == 'TPROF':
                        rootType = 'TProfile'
                    data['contents'][idx]['rootobj'] = t.ReadObject(eval(rootType + '.Class()'))

    return dict([(x['obj'], x) for x in data['contents'][1:] if 'obj' in x])

def dqm_get_samples(server, match, type="offline_data", ident=None):
    datareq = urllib2.Request(('%s/data/json/samples?match=%s') % (server, match))
    if ident:
        datareq.add_header('User-agent', ident)

    # Get data
    data = eval(re.sub(r"\bnan\b", "0", urllib2.build_opener(X509CertOpen()).open(datareq).read()),
               { "__builtins__": None }, {})
    ret = []
    for l in data['samples']:
        if l['type'] == type:
            ret += [ (int(x['run']), x['dataset']) for x in l['items'] ]
    return ret

def build_opener():
    """
    return x509 opener for certificate secured HTTPS urls
    """
    return urllib2.build_opener(X509CertOpen())

def query_DBS(opener, server, query):
    """
    query DBSReader API for specific query
    """
    url = "%s?%s" % (server, query)
    req = urllib2.Request(url)

    data = opener.open(req).read()

    return json.loads(data)
