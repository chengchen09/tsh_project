#!/usr/bin/env python
"""Usage: httpget dest-path ftp-source-path"""

# Author: chen cheng
# Description: dowload etopo1 data

import urllib
import os
import re
import sys
from collections import deque

import support

def usage(progname):
	print "Usage: {0} dest-path source-url".format(progname)
	sys.exit(-1)


def http_download_dir():
	
	if len(sys.argv) < 3:
		usage(sys.argv[0])
	
	if sys.argv[1] == '-r':
		support.redo_failed(sys.argv[2], os.path.dirname(sys.argv[2]))
		sys.exit(0)

	dest_path = sys.argv[1]
	if os.path.exists(dest_path) == False:
		print 'The destination path is not founded or have no permission to access!'
		sys.exit(-1)
	source_url = sys.argv[2]

	if dest_path.endswith('/') == False:
		dest_path += '/'
	if source_url.endswith('/') == False:
		source_url += '/'
	
	dir_q = deque([])
	file_q = deque([])
	t = (source_url, dest_path)
	dir_q.append(t)

	while len(dir_q) > 0:
		(url, path) = dir_q.popleft()
		parse_etopo_url(url, path, dir_q, file_q)

	# write the file url into file
	fh = open(dest_path + 'file-url.log', 'w')
	for (url, path) in file_q:
		fh.write(url + ' ' + path + '\n')
	fh.close()

	support.thread_download_file(file_q, dest_path)

		
def parse_etopo_url(url, dest_path, dir_q, file_q):
	src = urllib.urlopen(url).read()
	pos = src.index('valign')
	src = src[pos + 6:]
	pos = src.index('valign')
#	i = 1	
	start = 0
	end = 0
	while(pos >= 0):
		src = src[pos + 6:]
#		print src
#		print '-------------------------------',i
#		i = i + 1
		pos = src.index('alt="[')
		pos += 6
#print src[pos+6: pos+9]
		start = src.index('href="')
		start += 6
		end = src.index('"', start)
		name = src[start: end]
		path = dest_path + name
		new_url = url + name
		t = (new_url, path)
		
		if src[pos: pos+3] == 'DIR':
			if os.path.exists(path) == False:				
				os.mkdir(path)
			dir_q.append(t)
		else:
			file_q.append(t)
			
		pos = src.find('valign')


if __name__ == '__main__':
	http_download_dir()

