#!/usr/bin/env python
"""This is the support module for httpget.py"""

# AUTHOR: chen cheng
# DATA: 2011/03/30

import urllib
import sys
import threading
from collections import deque


def redo_failed(failed_path, dest_path):
	file_q = deque([])
	
	failed_fh = open(failed_path, 'r')
	reload_failed(failed_fh, file_q)
	failed_fh.close()

	download_file(file_q, dest_path)

def download_file(file_q, dest_path):
	failed_fh = open(dest_path + 'failed.log', 'w')
	down_fh = open(dest_path + 'downloaded.log', 'a')
	count = 0
	num = len(file_q)
	for (url, path) in file_q:
		try: 
			count += 1
			print 'downloading ' + url
			urllib.urlretrieve(url, path)
			print path + ' downloaded'
			left = num - count
			print('There are {0} files left'.format(left))
			down_fh.write(url + ' ' + path + '\n')
		except:
			print path + ' downloading faild'
			failed_fh.write(url + ' ' + path + '\n')
			
	failed_fh.close()
	down_fh.close()

def thread_download_file(file_q, dest_path):
	failed_fh = open(dest_path + 'failed.log', 'w')
	down_fh = open(dest_path + 'downloaded.log', 'a')
	thread_num = 4	# define the num of thread

	lock = threading.Lock();
	thds = []
	count = [0, 0]

	for i in range(thread_num):
		t = threading.Thread(target=thd_main, args=(file_q, failed_fh, down_fh, count, lock))
		thds.append(t)
		thds[i].start()
	for t in thds:
		t.join()

	failed_fh.close()
	down_fh.close()


def thd_main(file_q, failed_fh, down_fh, count, lock):

	len_files = len(file_q)
	while(1):
		
		lock.acquire()
		if count[0] < len_files:
			try:
				(url, path) = file_q[count[0]]
				count[0] += 1
				print 'downloading ' + url
				lock.release()

				urllib.urlretrieve(url, path)
		
				lock.acquire()
				print path + ' downloaded'
				count[1] += 1
				left = len_files - count[1]
				print('There are {0} files left'.format(left))
				down_fh.write(url + ' ' + path + '\n')
				lock.release();
			except:
				lock.acquire()
				print path + ' downloading faild'
				failed_fh.write(url + ' ' + path + '\n')
				lock.release()
		else:
			lock.release()
			return


def reload_failed(failed_fh, file_q):
	for line in failed_fh:
		line = line.rstrip('\n')
		(url, path) = line.split()
		file_q.append((url, path))

	
if __name__ == '__main__':
	file_q = deque([])
	with open(sys.argv[1], 'r') as fh:
		reload_failed(fh, file_q)
		print file_q
