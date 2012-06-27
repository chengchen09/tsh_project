#!/usr/bin/env python
"""convert netcdf3 file to hdf5 file"""

import sys
import netCDF4
import numpy
import h5py


def usage(progname):
	print "Usage: {0} netcdf3-file hdf5-file".format(progname)
	sys.exit(1)


def nc32h5(nc3path, h5path):
	nc3_fh = netCDF4.Dataset(nc3path, 'r', format='NETCDF3_CLASSIC')
	h5_fh = h5py.File(h5path, 'w')

	# global attributes
	attrs = []
	get_attrs(nc3_fh, attrs)
	set_attrs(h5_fh, attrs)

	# variables
	for name in nc3_fh.variables.keys():
		ncvar_h = nc3_fh.variables[name]
		h5var_h = h5_fh.create_dataset(name, ncvar_h.shape, dtype=ncvar_h.dtype, data=ncvar_h)
		
		# attributes
		attrs = []
		get_attrs(ncvar_h, attrs)
		set_attrs(h5var_h, attrs)

	nc3_fh.close()
	h5_fh.close()


def get_attrs(obj_h, attrs):
	for name in obj_h.ncattrs():
		attr = obj_h.getncattr(name)
		attrs.append((name, attr))


def set_attrs(obj_h, attrs):
	for (name, attr) in attrs:
		obj_h.attrs[name] = attr


def main():
	if len(sys.argv) < 3:
		usage(sys.argv[0])
	
	nc32h5(sys.argv[1], sys.argv[2])


if __name__ == '__main__':
	main()
