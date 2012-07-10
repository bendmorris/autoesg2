import urllib2
from xml.dom.minidom import parseString
from parameters import facets
import os


RESULT_LIMIT = 10000
RESULTS_FILE = 'files_to_download.txt'
urlstring = "http://pcmdi9.llnl.gov/esg-search/search?"

def format_url_string(limit, *args):
    arguments = zip((facet[0] for facet in facets), args)
    return urlstring + '&'.join('%s=%s' % (a, b) for (a, b) in arguments) + ('&limit=%s' % limit)

def get_str_from_doc(doc, string):
    arrs = doc.getElementsByTagName('arr')
    for arr in filter(lambda a: a.getAttribute('name')==string, arrs):
        strs = [this_str.toxml().replace('<str>', '').replace('</str>', '') for this_str in arr.getElementsByTagName('str')]
        return strs

def check(results_file, facets_done, docs, *facet_args):
    if facet_args in facets_done: return
    print '*'*len(facet_args), ':'.join(facet_args)

    if len(facet_args) == len(facets):
        # we have values for all facets, so save list of URLs
        print 'downloading', facet_args
        urls = download(docs, *facet_args)
        results_file.write('\n%s,%s' % (':'.join(facet_args), ';'.join(urls)))
        results_file.flush()
        facets_done.add(facet_args)
        return
    else:
        # find out which options for the next facet are wanted
        next_facet_name = facets[len(facet_args)][0]
        next_facet_options = facets[len(facet_args)][1]
        available_options = {}
        for doc in docs: 
            arrs = doc.getElementsByTagName('arr')
            for arr in filter(lambda a: a.getAttribute('name')==next_facet_name, arrs):
                strs = arr.getElementsByTagName('str')
                for this_str in strs:
                    key = str(this_str.toxml().replace('<str>', '').replace('</str>', '')).lower()
                    if not this_str in available_options:
                        available_options[key] = []
                    available_options[key].append(doc)
        print available_options.keys()
        options = filter(lambda o: o.lower() in available_options, next_facet_options)
        print 'found', len(options), next_facet_name+'s: ',options
        for option in options:
            check(results_file, facets_done, available_options[option.lower()], *(facet_args + (option,)))
        facets_done.add(facet_args)


def download(docs, *facet_args):
    urls = []
    for doc in docs: 
        formats = get_str_from_doc(doc, 'format')
        if not [True for format in formats if 'netcdf' in format.lower()]: return
        these_urls = get_str_from_doc(doc, 'url')
        urls += [url for url in these_urls if 'application/xml+thredds|catalog' in url.lower()]
    return urls


facets_done = set()
try:
    results_file = open(RESULTS_FILE, 'r')
    results_file.readline()
    for line in results_file:
        if line:
            facet = tuple(line.split(',')[0].split(':'))
            facets_done.add(facet)
    results_file.close()
except:
    results_file = open(RESULTS_FILE, 'w')
    results_file.write('facet,url')
results_file = open(RESULTS_FILE, 'a')

for model in facets[1][1]:
    for experiment in facets[2][1]:
        facet_args = ('CMIP5', model, experiment)
        model_path = ('%s.%s.xml' % (model, experiment)).lower()
        if os.path.exists(model_path):
            file = open(model_path, 'r')
            data = file.read()
            file.close()
        else:
            url = format_url_string(RESULT_LIMIT, *facet_args)
            print '==>', url
            file = urllib2.urlopen(url)
            data = file.read()
            file.close()

            file = open(model_path, 'w')
            file.write(data)
            file.close()

        dom = parseString(data)
        result = dom.getElementsByTagName('result')[0]

        num_found = int(result.getAttribute('numFound'))
        print 'found', num_found, 'results'
        if num_found >= RESULT_LIMIT:
            raise Exception('reached max number of results')
        
        if num_found:
            docs = result.getElementsByTagName('doc')
            check(results_file, facets_done, docs, *facet_args)