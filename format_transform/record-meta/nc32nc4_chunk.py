#!/usr/bin/env python
"""convert netcdf3 file to chunked netcdf4 file"""

import sys
import netCDF4


def usage(progname):
	print "Usage: {0} netcdf3-infile netcdf4-outfile".format(progname)
	sys.exit(1)


def get_chdims(ncvar):
	ch_dims = []
	if 2 == ncvar.ndim:
		ch_dims = [100, 100]
	elif 3 == ncvar.ndim:
		ch_dims = [100, 100, 100]

	vardims = list(ncvar.shape)
	for i in range(ncvar.ndim):
		if ch_dims[i] > vardims[i]:
			ch_dims = vardims
			break

	return ch_dims


def nc32nc4_chunk(nc3path, nc4path):
	nc3_fh = netCDF4.Dataset(nc3path, 'r', format='NETCDF3_CLASSIC')
	nc4_fh = netCDF4.Dataset(nc4path, 'w', format='NETCDF4')

	# global attributes
	attrs = []
	get_nc3_attrs(nc3_fh, attrs)
	set_nc4_attrs(nc4_fh, attrs)

	# dimension
	for dimname, dimobj in nc3_fh.dimensions.items():
		nc4_fh.createDimension(dimname, len(dimobj))

	# variables
	for name in nc3_fh.variables.keys():
		nc3var_h = nc3_fh.variables[name]
		dimnames = nc3var_h.dimensions

		if nc3var_h.ndim > 1:
			ch_dims = get_chdims(nc3var_h)
		
			if len(ch_dims) < 2:
				print 'dimension length is not supported'
				sys.exit(1)

			nc4var_h = nc4_fh.createVariable(name, nc3var_h.dtype, dimensions=tuple(dimnames),
						zlib=True, complevel=6, chunksizes=tuple(ch_dims))
		else:
			nc4var_h = nc4_fh.createVariable(name, nc3var_h.dtype, dimensions=tuple(dimnames))
		
		nc4var_h[...] = nc3var_h

		# variable attributes
		attrs = []
		get_nc3_attrs(nc3var_h, attrs)
		set_nc4_attrs(nc4var_h, attrs)

	nc3_fh.close()
	nc4_fh.close()


def get_nc3_attrs(obj_h, attrs):
	for name in obj_h.ncattrs():
		value = obj_h.getncattr(name)
		attrs.append((name, value))


#TODO: set_attrs: _FillValue change to missing value, because the variable attribute has defined the _FillValue. missing_value is in the COARDS convention, and it is not recommanded in CF conventions but supported
def set_nc4_attrs(obj_h, attrs):
	for (name, value) in attrs:
		if 0 == cmp(name, '_FillValue'):
			obj_h.missing_value = value
		else:
			obj_h.setncattr(name, value)

def main():
	if len(sys.argv) < 3:
		usage(sys.argv[0])

	if 0 == cmp(sys.argv[1], sys.argv[2]):
		print 'nc3 infile can\'t be the same with nc4 outfile'
		sys.exit(1)

	nc32nc4_chunk(sys.argv[1], sys.argv[2])


if __name__ == '__main__':
	main()
