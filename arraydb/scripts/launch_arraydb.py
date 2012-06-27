#!/usr/bin/env python

import sys
import os

def usage():
    print 'Usage: lauch_arraydb.py role(master or worker)'


def main():
    if len(sys.argv) < 2:
	usage()
	sys.exit(1)

    local_role = sys.argv[1]
    nodes_addr_dict = {}
    nodes_num = 0
    nodes_added = 0
    data_dir = ''
    local_addr = ''
    nodeid = ''
    
    if local_role == 'master':
	nodeid = '0'
    else:
	nodeid = local_role[7:]
    print 'local id: ', nodeid

    fh = open('./arraydb.conf', 'r')
    if (fh):
	for line in fh:
	    # nodes_num
	    if line.find('nodes_num') == 0:
		numstr = (line.split('='))[1]
		if numstr == None:
		    print 'nodes_num: wrong format'
		numstr = numstr.strip('\n')
		numstr = numstr.strip()
		nodes_num = int(numstr)
	    
	    # role
	    if line.find('master') == 0 or line.find('worker') == 0:
		
		(role, address) = line.split('=')
		role = role.strip()

		if address == None:
		    print 'master: wrong format'
		    sys.exit(1)
		address = address.strip('\n')
		address = address.strip()
		(ip, port) = address.split(':')
		ip = ip.strip()
		port = port.strip()
	
		nodes_addr_dict[role] = address
		#nodes_addr_list.append(dict([('role', role), 
		 #   ('ip', ip), ('port', port)]))
		nodes_added = nodes_added + 1
		if role == local_role:
		    local_addr = ip + ':' + port

	    # data_dir
	    if line.find('data_dir') == 0:
		(tmp, path) = line.split('=')
		
		if path == None:
		    print 'data_dir: wrong format'
		    sys.exit(1)
		path = path.strip('\n')
		path = path.strip()
		data_dir = path
   
    fh.close()

    if nodes_num != nodes_added:
	print 'nodes num is not exact'
	print 'nodes_num: ', nodes_num
	print 'nodes_added: ', nodes_added
	sys.exit(1)

    # debug
    for role in nodes_addr_dict.keys():
	print role, nodes_addr_dict[role]

    cmd = './arraydb_server -l ' + local_addr + ' -i ' + nodeid + ' -m ' + nodes_addr_dict['master']
    for role in nodes_addr_dict.keys():
	if role.startswith('worker'):
	    cmd = cmd + ' -w ' + nodes_addr_dict[role]

    #cmd = cmd + ' &'
    print cmd

    os.system(cmd)

if __name__ == '__main__':
    main()
