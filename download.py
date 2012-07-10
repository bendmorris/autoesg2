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
from getpass import getpass
from xml.dom.minidom import parseString
from config import *
    

LOG_FILE = "download_log.txt"
MAX_THREADS = 10
USER = 'benmorris'
HOME_PATH = '/home4/%s' % USER
CERT_PATH = "%s/.esg/certificates" % HOME_PATH
CRED_PATH = "%s/.esg/credentials.pem" % HOME_PATH
COOKIE_PATH = "%s/.esg/cookies" % HOME_PATH
if not os.path.exists(LOG_FILE): open(LOG_FILE, 'w').close()

n = 1
while n < len(sys.argv):
    a = sys.argv[n]
    
    try:
        if a == "-f" or a == "--file":
            RESULTS_FILE = sys.argv[n+1]
            n += 1
        elif a == "-t" or a == "--threads":
            MAX_THREADS = int(sys.argv[n+1])
            n += 1
        else:
            print "Unknown argument: " + a
        

    except IndexError:
        print "No argument to " + a
        pass
        
    n += 1        


if not os.path.exists(RESULTS_FILE): open(RESULTS_FILE, 'w').close()


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

#log_file = open(LOG_FILE, "a")

class Logger:
    def write(self, t):
        if t.replace("\n", "").strip():
            message = time.strftime('%x %X ') + t + "\n"
            sys.__stdout__.write(message)
            sys.__stdout__.flush()
    def flush(self):pass
        

class Downloader:
    def __init__(self, n, this_file, finished_log):
        Thread.__init__(self)
        self.n = n
        self.this_file = this_file
        self._started = False
        self._finished = False
        self._downloading = False
        self.finished_log = finished_log
        self.downloaded = 0
        self.counted = False
        self.temp_file_path = None
        self.daemon = True
        self.start_time = time.time()

    def started(self):
        return self._started
        
    def downloading(self):
        return self._downloading
        
    def finished(self):
        return self._finished
        
    def start(self):
        self._started = True
        Thread.start(self)
        
    def run(self):
        print str(self.n) + ": " + str(self.this_file[0]) + " started."

        this_file = self.this_file
        b = self.browser
        try:
            download_path = os.path.join(next_data_store(), '/'.join(this_file[0].split(':')[:3]))
            make_dirs(download_path)
            
            response = urllib2.urlopen(this_file[1])
            data = response.read()
            response.close()
                        
            dom = parseString(data)            
            root_dataset = dom.getElementsByTagName('dataset')[0]
            datasets = root_dataset.getElementsByTagName('dataset')[0]
            urls = [str(dataset.getAttribute('urlPath')) for dataset in datasets]
            download_urls = [os.path.join('/'.join(this_file[1].split('/')[:-1]), url) for url in urls]
            print 'This result has %s files to download.' % len(download_urls)
            
            for download_url in download_urls
                filename = download_url.split('/')[-1].split('?')[0]
            
                if not filename[-3:] == ".nc":
                    raise Exception("Download target " + filename + " is not a .nc file.")

                for data_store in DATA_STORES:
                    if os.path.exists(data_store + this_file[0] + filename):
                        exists = data_store + this_file[0]

                if exists:
                    print filename + " already exists at " + exists
                else:
                    path = download_path + filename
                    make_dirs(self.temp_file_path)
                    self.temp_file_path = os.path.join(download_path, "temp." + str(self.n))
                    self.final_destination = path
                    save_file = open(self.temp_file_path, "wb")

                    wget_string = """wget -c --ca-directory=%s --certificate=%s
--private-key=%s
--save-cookies=%s 
--load-cookies=%s 
-q 
-O %s
%s
""".replace('\n', ' ') % (CERT_PATH, CRED_PATH, CRED_PATH, COOKIE_PATH, COOKIE_PATH, self.temp_file_path, download_url)
                
                    print str(self.n) + ": Downloading " + filename + " to " + download_path + " ..."
                    success = subprocess.call(wget_string.split())
                    if success != 0: 
                        wget_string += " --no-check-certificate"
                        success = subprocess.call(wget_string.split())
                        if success != 0: 
                            os.remove(self.temp_file_path)
                            raise Exception("WGET operation failed, status %s." % success)

                    shutil.move(self.temp_file_path, path)
                
                    print str(self.n) + ": Download finished."

                self.finished_log.write(', '.join(this_file) + "\n")
                self.finished_log.flush()
                
        except Exception as e:
            print str(self.n) + ": " + str(this_file)
            print str(self.n) + ": " + str(e)
        
        
        
            
    
def wait(waittime):
    itime = time.time()
    while time.time() - itime < waittime:
        pass
    
class NoHistory(object):
    def add(self, *a, **k): pass
    def clear(self): pass
    def close(self): pass
    
sys.stdout = sys.stderr = Logger()

def main():
    downloaders = []
    try:
        results = open(RESULTS_FILE, "r")
        all_files = []
        for line in results:
            if line and len(line.split(",")) > 1:
                this_file = [s.strip() for s in line.split(",")]
                this_file[0] = this_file[0].lower().replace("cmip5", "CMIP5").split(':')
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
        
        print str(n) + " files to download"
        if n == 0:
            return

        n = 0

        finished = open(DOWNLOADS_FILE, "a")
        for this_file in all_files:
            if not this_file in already_downloaded:
                n += 1
                downloaders.append(DownloadThread(n, this_file, finished))
        done = 0

        print "%s results to download." % n
        
        for downloader in downloaders:
            downloader.run()
                
        finished.close()
        
    except KeyboardInterrupt:
        for thread in threads:
            if thread.temp_file_path:
                try: os.remove(thread.temp_file_path)
                except: pass
        sys.exit()
    
            
total_downloaded = main()
print "All downloads finished."
print "Total downloaded: %.2f MB" % (total_downloaded / (1000. ** 2))
sys.stdout = sys.__stdout__
sys.stderr = sys.__stderr__
