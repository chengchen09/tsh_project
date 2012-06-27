#!/usr/bin/env python
"""Usage: ftpget dest-path ftp-source-path"""

import ftplib
import os
import re
import sys

def usage():
	print __doc__
	sys.exit(-1)

def download_all(ftp):
	filelist = []
	
	ftp.retrlines('LIST', filelist.append)
	print '---------------------------enter '+ dirname + '--------------------------------------'

	dirs = ftp.nlst()

	for line in filelist:
		fieldList = line.split(None, 8)
		flname = fieldList[-1]

		if len(fieldList) < 6:
			continue

		elif flname == '.' or flname == '..':
			continue		
		elif re.match('^d', fieldList[0]) is not None: 
			find_hdf5(ftp, flname)
		elif re.match('^-', fieldList[0]) is not None:
#			ret = flname.find('.h5')
			if re.match('.*\.h5$', filename) is not None and os.path.isfile(filedir+flname) == False:
				try:
					print 'start downloading file: ' + flname
					filehandler = open(filedir+flname, 'wb').write
					ftp.retrbinary('RETR ' + flname, filehandler)
					print 'end downloading file: ' + flname
				except ftplib.error_perm:
					print flname

	ftp.cwd('../')			# quit this dir
	print '---------------------------quit ' + dirname + '--------------------------------------'


def ftp_download_dir():
	
	if len(sys.argv) < 3:
		usage()

	dest_path = sys.argv[1]
	if os.path.exists(dest_path) == False:
		print 'The destination path is not founded or have no permission to access!'
		sys.exit(-1)
	
	ftpdir = ''
#	ftpaddr, ftpdir = split_ftp(sys.argv[2])
	ftpaddr = sys.argv[2]
	#try:
	ftp = ftplib.FTP(ftpaddr)
	ftp.login()
	if ftpdir != '':
		ftp.cwd(ftpdir)
#	except ftplib.all_errors:
#		print str(ftplib.all_errors)
#		sys.exit(-1)	
	dirlist = ftp.dir()
	print dirlist
#	download_all(ftp)

	ftp.quit()
	ftp.close()
	
	return

def split_ftp(ftpstr):
	ftpaddr = ''
	ftpdir = ''
	index = ftpstr.find('/')
	if index < 0:
		ftpaddr = ftpstr
		ftpdir = ''
	else:
		ftpaddr = ftpstr[:index]
		ftpdir = ftpstr[index:]
	
	print ftpaddr, ftpdir
	return ftpaddr, ftpdir

if __name__ == '__main__':
	ftp_download_dir()


