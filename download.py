#!/usr/bin/python
#
# script to automatically download CMIP data
#
# usage: python download.py [-u username] 
#                           [-p password]
#                           [-f path]
# -u    login username
# -p    login password
# -f    specify the path of the files_to_download file

import gc
import os
import re
import shutil
import subprocess
import sys
import time
import urllib2
import mechanize
from getpass import getpass
from xml.dom.minidom import parseString
from config import *
    

USER = 'benmorris'
HOME_PATH = '/home4/%s' % USER
CERT_PATH = "%s/.esg/certificates" % HOME_PATH
CRED_PATH = "%s/.esg/credentials.pem" % HOME_PATH
COOKIE_PATH = "%s/.esg/cookies" % HOME_PATH

b = mechanize.Browser()
b.set_handle_robots(False)


n = 1
while n < len(sys.argv):
    a = sys.argv[n]
    
    try:
        if a == "-f" or a == "--file":
            RESULTS_FILE = sys.argv[n+1]
            n += 1
        else:
            print "Unknown argument: " + a
        

    except IndexError:
        print "No argument to " + a
        pass
        
    n += 1        


if not os.path.exists(RESULTS_FILE): open(RESULTS_FILE, 'w').close()
if not os.path.exists(DOWNLOADS_FILE): open(DOWNLOADS_FILE, 'w').close()

def dir_fill(path):
    make_dirs(path)
    try:
        f = os.statvfs(path).f_bfree
        a = os.statvfs(path).f_blocks
        return float(a - f) / a
    except:
        return 1.0
    
def next_data_store():
    for data_store in DATA_STORES:
        
        if dir_fill(data_store) < 0.9:
            return data_store
    return ""
    
def make_dirs(path):
    try:
        os.makedirs(path)
    except:
        pass


class Downloader:
    def __init__(self, n, this_file, finished_log):
        self.n = n
        self.this_file = this_file
        self.finished_log = finished_log

    def run(self):
        print self.n, self.this_file

        this_file = self.this_file
        download_path = os.path.join(next_data_store(), '/'.join(this_file[0].split(':')[:3]))
        make_dirs(download_path)
            
        b.open(this_file[1])
        data = b.response().read()
        b.response().close()

        dom = parseString(data)            
        root_dataset = dom.getElementsByTagName('dataset')[0]
        datasets = root_dataset.getElementsByTagName('dataset')
        urls = filter(lambda f: f.endswith('.nc'), [str(dataset.getAttribute('urlPath')) for dataset in datasets])
        download_urls = [os.path.join('/'.join((this_file[1].split('#')[0]).split('/')[:-1]), url) for url in urls]
        print download_urls
        print 'This result has %s files to download.' % len(download_urls)
            
        for download_url in download_urls:
            filename = download_url.split('/')[-1].split('?')[0]
            
            if not filename[-3:] == ".nc":
                print "Download target " + filename + " is not a .nc file."
                continue

            exists = False
            for data_store in DATA_STORES:
                if os.path.exists(data_store + this_file[0] + filename):
                    exists = data_store + this_file[0]
                    break

            if exists:
                print filename + " already exists at " + exists
            else:
                path = download_path + filename
                temp_file_path = os.path.join(download_path, "temp")
                self.final_destination = path
                save_file = open(temp_file_path, "wb")

                wget_string = """wget -c --ca-directory=%s --certificate=%s
--private-key=%s
--save-cookies=%s 
--load-cookies=%s 
-q
-O %s
%s
""".replace('\n', ' ') % (CERT_PATH, CRED_PATH, CRED_PATH, COOKIE_PATH, COOKIE_PATH, temp_file_path, download_url)
                
                print str(self.n) + ": Downloading " + filename + " to " + download_path + " ..."
                try:
                    success = subprocess.call(wget_string.split())
                except Exception as e:
                    print "EXCEPTION:", download_url, e
                if success != 0: 
                    wget_string += " --no-check-certificate"
                    success = subprocess.call(wget_string.split())
                    if success != 0: 
                        os.remove(temp_file_path)
                        raise Exception("WGET operation failed, status %s." % success)

                shutil.move(temp_file_path, path)
                
                print str(self.n) + ": Download finished."

            self.finished_log.write(', '.join(this_file) + "\n")
            self.finished_log.flush()
        
        
        
            
    
def wait(waittime):
    itime = time.time()
    while time.time() - itime < waittime:
        pass
    
class NoHistory(object):
    def add(self, *a, **k): pass
    def clear(self): pass
    def close(self): pass
    
def main():
    downloaders = []
    results = open(RESULTS_FILE, "r")
    results.readline()
    all_files = []
    for line in results:
        if line and len(line.split(",")) > 1:
            this_file = [s.strip() for s in line.split(",")]
            this_file[0] = this_file[0].lower().replace("cmip5", "CMIP5")
            all_files.append(tuple(this_file))
    results.close()

    already_downloaded = set()
    finished = open(DOWNLOADS_FILE, "r")
    for line in finished:
        if line and len(line.split(",")) > 1:
            this_file = [s.strip() for s in line.split(",")]
            this_file[0] = this_file[0].lower().replace("cmip5", "CMIP5")
            already_downloaded.add(tuple(this_file))
    finished.close()

    all_files = [f for f in all_files if not f in already_downloaded]
    n = len(all_files)
        
    if n == 0:
        return

    n = 0

    finished = open(DOWNLOADS_FILE, "a")
    for this_file in all_files:
        if not this_file in already_downloaded:
            n += 1
            downloaders.append(Downloader(n, this_file, finished))
    done = 0

    print "%s results to download." % n
      
    for downloader in downloaders:
        downloader.run()
                
    finished.close()
        
            
main()
print "All downloads finished."
