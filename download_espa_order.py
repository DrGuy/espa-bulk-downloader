#!/usr/bin/env python

"""
Author: David Hill
Date: 01/31/2014
Purpose: A simple python client that will download all available (completed) scenes for
         a user order(s).

Requires: Python feedparser and standard Python installation.     

Version: 1.0

Changes: 

30 June 2016: Guy Serbin added support for Python 3.x and download progress indicators.
24 August 2016: Guy Serbin added:
1. The downloads will now tell you which file number of all available scenes is being downloaded.
2. Added a try/except clause for cases where the remote server closes the connection during a download.
3. Increased a delay range from 5-30 seconds to 1-60 seconds during downloads.
4. The progress bar has not been implemented as it only pops up at the end of a file download. However, the function is still in place.

1 September 2016: Guy Serbin added:
1. Modified download method from shutil.copyfileobj to in-file copyfileobj function, based upon code in http://stackoverflow.com/questions/1517616/stream-large-binary-files-with-urllib2-to-file
2. This function now works with the download progress bar.
3. Diagnostic information like file size in MB and download start time added.
4. A timeout function for 30 minutes has been added using multiprocessing.Progress, and set to retry downloads five times before failing.
Note: I have only tested this on Python 3.4 running on a Windows 64 bit machine. 

2 September 2015: Guy Serbin modified LocalStorage class to close opened URL objects to improve memory usage.

"""

import sys
import feedparser
if sys.version_info[0] !=3:
    import urllib2 as ul
else:
    import urllib.request as ul
#    import urllib.error
import argparse
import shutil
import os
import time
import random
import base64
from multiprocessing import Process

CHUNK = 16

def drawProgressBar(first_byte, file_size, barLen = 40):
    # This function was adapted from Jacob Tsui's answer on http://stackoverflow.com/questions/3002085/python-to-print-out-status-bar-and-percentage
    # I have no idea how to get this to draw with download progression- it only appears after the download has completed.
    percent = float(first_byte)/ float(file_size)   
    sys.stdout.write("\r")
    progress = ""
    for i in range(barLen):
        if i < int(barLen * percent):
            progress += "="
        else:
            progress += " "
    sys.stdout.write("[ %s ] %.2f%% (%d/%d)" % (progress, percent * 100, first_byte, file_size))
    sys.stdout.flush()

def copyfileobj(source, target, first_byte, file_size): # This replaces shutil.copyfileobj
    while True:
        chunk = source.read(CHUNK)
        if not chunk: break
        target.write(chunk)
        first_byte += len(chunk)
        drawProgressBar(first_byte, file_size)
    


class SceneFeed(object):
    """SceneFeed parses the ESPA RSS Feed for the named email address and generates
    the list of Scenes that are available"""
    
    def __init__(self, email, username, password, host="http://espa.cr.usgs.gov"):
        """Construct a SceneFeed.
        
        Keyword arguments:
        email -- Email address orders were placed with
        host  -- http url of the RSS feed host
        """
        if not host:
            host = "http://espa.cr.usgs.gov"

        self.host = host
        self.email = email
        self.user = username
        self.passw = password

        self.feed_url = "%s/ordering/status/%s/rss/" % (self.host, self.email)

    def get_items(self, orderid='ALL'):
        """get_items generates Scene objects for all scenes that are available to be
        downloaded.  Supply an orderid to look for a particular order, otherwise all
        orders for self.email will be returned"""
        
        #yield Scenes with download urls

        auth_str = "%s:%s" % (self.user, self.passw)
        if sys.version_info[0] !=3:
            bauth = base64.b64encode(auth_str)
        else:
            bauth = base64.b64encode(auth_str.encode())

        feed = feedparser.parse(self.feed_url, request_headers={"Authorization": bauth})
        numfiles=len(feed.entries)
        print('There are a total of %d files available for download.'%numfiles)
        
        if feed.status == 403:
            print("user authentication failed")
            exit()

        if feed.status == 404:
            print("there was a problem retrieving your order. verify your orderid is correct")
            exit()
            
        filenum = 1
        for entry in feed.entries:

            #description field looks like this
            #'scene_status:thestatus,orderid:theid,orderdate:thedate'
            scene_order = entry.description.split(',')[1].split(':')[1]

            #only return values if they are in the requested order            
            if orderid == "ALL" or scene_order == orderid:
                yield Scene(entry.link, scene_order, filenum, numfiles)
            
            filenum += 1
            
                
class Scene(object):
    
    def __init__(self, srcurl, orderid, filenum, numfiles):
    
        self.srcurl = srcurl
    
        self.orderid = orderid
        
        parts = self.srcurl.split("/")
     
        self.filename = parts[len(parts) - 1]
        
        self.name = self.filename.split('.tar.gz')[0]
        
        self.filenum = filenum
        
        self.numfiles = numfiles
        
                  
class LocalStorage(object):
    
    def __init__(self, basedir):
        self.basedir = basedir

    def directory_path(self, scene):
        return ''.join([self.basedir, os.sep, scene.orderid, os.sep])
        
    def scene_path(self, scene):
        return ''.join([self.directory_path(scene), scene.filename])
    
    def tmp_scene_path(self, scene):
        return ''.join([self.directory_path(scene), scene.filename, '.part'])
    
    def is_stored(self, scene):        
        return os.path.exists(self.scene_path(scene))        
    
    def store(self, scene):
        
        if self.is_stored(scene):
            print('Scene already exists on disk, skipping.')
            return
                    
        download_directory = self.directory_path(scene)
        
        #make sure we have a target to land the scenes
        if not os.path.exists(download_directory):
            os.makedirs(download_directory)
            print ("Created target_directory:%s" % download_directory)
        
        req = ul.Request(scene.srcurl)
        req.get_method = lambda: 'HEAD'

        head = ul.urlopen(req)
        file_size = int(head.headers['Content-Length'])

        if os.path.exists(self.tmp_scene_path(scene)):
            first_byte = os.path.getsize(self.tmp_scene_path(scene))
        else:
            first_byte = 0

        print ("Downloading %s (%03f MB), file number %d of %d, to: %s (%s)" % (scene.name, float(file_size)/(1024**2), scene.filenum, scene.numfiles, download_directory, time.strftime('%Y-%m-%d %H:%M:%S',time.localtime())))

        while first_byte < file_size:
            try: # Added this to keep the script from crashing if the remote host closes the connection. Instead, it moves on to the next file.
                first_byte = self._download(first_byte, file_size)
#                drawProgressBar(first_byte, file_size)
                time.sleep(random.randint(10, 30)) # Expanded the range of this, hopefully to reduce the amount of timeouts that may occur with downloading. Not sure if it will have any real effect.
            except Exception as e:
                print(str(e))
                break
        if first_byte >= file_size:
            os.rename(self.tmp_scene_path(scene), self.scene_path(scene))
            # Free up memory
            head = None
            req = None

    def _download(self, first_byte, file_size): 
#        try:
            req = ul.Request(scene.srcurl) 
            req.headers['Range'] = 'bytes={}-'.format(first_byte)
    
            with open(self.tmp_scene_path(scene), 'ab') as target:
                source = ul.urlopen(req)
                copyfileobj(source, target, first_byte, file_size) # shutil.copyfileobj(source, target)
    
            req = None
            source = None
            return os.path.getsize(self.tmp_scene_path(scene))
#        except Exception as e:
#            print(str(e))


if __name__ == '__main__':
    e_parts = ['ESPA Bulk Download Client Version 1.0.0. [Tested with Python 2.7]\n']
    e_parts.append('Retrieves all completed scenes for the user/order\n')
    e_parts.append('and places them into the target directory.\n')
    e_parts.append('Scenes are organized by order.\n\n')
    e_parts.append('It is safe to cancel and restart the client, as it will\n')
    e_parts.append('only download scenes one time (per directory)\n')
    e_parts.append(' \n')
    e_parts.append('*** Important ***\n')
    e_parts.append('If you intend to automate execution of this script,\n')
    e_parts.append('please take care to ensure only 1 instance runs at a time.\n')
    e_parts.append('Also please do not schedule execution more frequently than\n')
    e_parts.append('once per hour.\n')
    e_parts.append(' \n')
    e_parts.append('------------\n')
    e_parts.append('Examples:\n')
    e_parts.append('------------\n')
    e_parts.append('Linux/Mac: ./download_espa_order.py -e your_email@server.com -o ALL -d /some/directory/with/free/space\n\n') 
    e_parts.append('Windows:   C:\python27\python download_espa_order.py -e your_email@server.com -o ALL -d C:\some\directory\with\\free\space')
    e_parts.append('\n ')
    epilog = ''.join(e_parts)
 
    parser = argparse.ArgumentParser(epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter)
    
    parser.add_argument("-e", "--email", 
                        required=True,
                        help="email address for the user that submitted the order)")
                        
    parser.add_argument("-o", "--order",
                        required=True,
                        help="which order to download (use ALL for every order)")
                        
    parser.add_argument("-d", "--target_directory",
                        required=True,
                        help="where to store the downloaded scenes")

    parser.add_argument("-u", "--username",
                        required=True,
                        help="EE/ESPA account username")

    parser.add_argument("-p", "--password",
                        required=True,
                        help="EE/ESPA account password")

    parser.add_argument("-v", "--verbose",
                        required=False,
                        help="be vocal about process")

    parser.add_argument("-i", "--host",
                        required=False)

    parser.add_argument("-t", "--timeout",
                        required=False, default = 30, help = "Download timeout in minutes", type = int)

    args = parser.parse_args()
    
    storage = LocalStorage(args.target_directory)
    
    timeout =args.timeout * 60 # convert timeout from minutes to seconds
    
    print('Retrieving Feed')
    sf = SceneFeed(args.email, args.username, args.password, args.host).get_items(args.order)
    for scene in sf:
        if not storage.is_stored(scene):
            numtries = 1
            print('\nNow processing scene %s.'%(scene.name))
            while numtries < 6:
                print('Attempt %d of 5'%numtries)
                p = Process(target = storage.store(scene))
                p.start()
                # wait until process finishes or times out
                p.join(timeout)
#                storage.store(scene)
                # If thread is still active
                if p.is_alive():
                    print('Error: A download timeout has occurred.')
                    # terminate the process
                    p.terminate()
                    p.join()
                if storage.is_stored(scene):
                    numtries = 6
                else:
                    numtries += 1
            if numtries == 6 and not storage.is_stored(scene):
                print('ERROR: Download failed.')
        
