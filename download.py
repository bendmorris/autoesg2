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

from threading import Thread, Lock
import gc
import os
import re
import shutil
import subprocess
import sys
import time
from urllib2 import URLError
from getpass import getpass
import mechanize
from config import *
    

LOG_FILE = "download_log.txt"
MAX_THREADS = 10
CERT_PATH = "/home4/benmorris/.esg/certificates"
CRED_PATH = "/home4/benmorris/.esg/credentials.pem"
COOKIE_PATH = "/home4/benmorris/.esg/cookies"

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
downloads_started = set()
output_lock = Lock()
finished_lock = Lock()
downloads_started_lock = Lock()


class Logger:
    def write(self, t):
        output_lock.acquire()
        try:
            if t.replace("\n", "").strip():
                message = time.strftime('%x %X ') + t + "\n"
                sys.__stdout__.write(message)
                sys.__stdout__.flush()
        except:
            output_lock.release()
            raise
        output_lock.release()
    def flush(self):pass
        

class DownloadThread(Thread):
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
        print str(self.n) + ": " + str(self.this_file[0]) + " started."
        self._started = True
        self.browser = mechanize.Browser(history=NoHistory(), )
        self.browser.set_handle_robots(False)
        Thread.start(self)
        
    def run(self):
        this_file = self.this_file
        b = self.browser
        try:
            download_path = next_data_store() + this_file[0]
            make_dirs(download_path)
            
            response = b.open(this_file[1])
            if not ".htm" in b.geturl(): raise Exception("Accessed URL, but didn't find a .htm document")

            html = response.read()
                        
            urls = re.findall(r"http://.*\.nc", html)
            download_url = urls[0]
            filename = download_url.split('/')[-1].split('?')[0]
            
            if not filename[-3:] == ".nc":
                raise Exception("Download target " + filename + " is not a .nc file.")

            downloads_started_lock.acquire()
            try:
                exists = filename in downloads_started
            except:
                downloads_started_lock.release()
                raise
            downloads_started_lock.release()

            if not exists:
                for data_store in DATA_STORES:
                    if os.path.exists(data_store + this_file[0] + filename):
                        exists = data_store + this_file[0]

            if exists:
                print filename + " already exists at " + exists
            else:
                downloads_started_lock.acquire()
                try:
                    downloads_started.add(filename)
                except:
                    downloads_started_lock.release()
                    raise
                downloads_started_lock.release()            

                path = download_path + filename
                make_dirs(self.temp_file_path)
                self.temp_file_path = os.path.join(download_path, "temp." + str(self.n))
                self.final_destination = path
                save_file = open(self.temp_file_path, "wb")
                self._downloading = True

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

            finished_lock.acquire()
            try:
                self.finished_log.write(', '.join(this_file) + "\n")
                self.finished_log.flush()
            except:
                finished_lock.release()
                raise
            finished_lock.release()
                
        except Exception as e:
            print str(self.n) + ": " + str(this_file)
            print str(self.n) + ": " + str(e)

        self._finished = True
        b.close()        
        
        
        
            
    
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
    threads = []
    try:
        results = open(RESULTS_FILE, "r")
        all_files = []
        for line in results:
            if line and len(line.split(",")) > 1:
                this_file = [s.strip() for s in line.split(",")]
                this_file[0] = this_file[0].lower().replace("cmip5", "CMIP5")
                all_files.append(tuple(this_file))
        results.close()

        already_downloaded = set()
        if not os.path.exists(DOWNLOADS_FILE): open(DOWNLOADS_FILE, "w").close()
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
                threads.append(DownloadThread(n, this_file, finished))
        done = 0
        
        start_time = time.time()
        loops = 0
        total_size = 0
        last_size = 0
        total_downloaded = 0
        while threads:
            loops += 1
            finished_threads = [t for t in threads if t.finished()]
            done = len(finished_threads)
            
            running = len([t for t in threads if t.started() and not (t.finished() or time.time() - 
t.start_time > 300)])
            downloading = len([t for t in threads if t.downloading() and not t.finished()])                                
            
            if running < MAX_THREADS:
                for t in [t for t in threads
                          if not t.started()][:MAX_THREADS - running]:
                    t.start()
                    running += 1

            if loops % 1 == 0:
                total_size = 0

                for t in threads:
                    if t.started() and not t.finished():
                        try: total_size += os.path.getsize(t.temp_file_path)
                        except: pass
                    elif t.finished() and not t.counted:
                        try:
                            t.counted = True 
                            last_size -= os.path.getsize(t.final_destination)
                            total_downloaded += os.path.getsize(t.final_destination)
                        except: pass

                elapsed_time = time.time() - start_time

                print ("Downloading " + str(downloading) + " of " + str(len(threads) - done) + " files, " +
                       ("%.2f" % ((total_size - last_size) / (elapsed_time * (1000. ** 2)))) + " MB/sec, " + 
                       ("total %.2f MB" % ((total_downloaded + total_size) / (1000. ** 2))))
                start_time = time.time()
                last_size = total_size
                loops = 0
                
            for t in finished_threads:
                threads.remove(t)
                    
            wait(10)
                
        finished.close()
        return total_downloaded
        #main()
        
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
