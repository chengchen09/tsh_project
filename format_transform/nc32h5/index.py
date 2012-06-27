#!/usr/bin/env python
"""create the rtree index for a hdf dataset"""

import sys, os
import rtree
import h5py
import cPickle
import pickle

class FastRtree(rtree.Rtree):
	def dumps(self, obj):
		return cPickle.dumps(obj, -1)


def create_index(h5path, variable, dimensions):
	h5_fh = h5py.File(h5path, 'r')
	var_id = h5_fh[variable]
	dim_ids = []
	for dim in dimensions:
		dim_id = h5_fh[dim]
		dim_ids.append(dim_id)
	
	chunk_objs = []
	create_2D_objs(var_id, dim_ids, chunk_objs)
	
	# create index
	p = rtree.index.Property()

	idx = rtree.Rtree(h5path, properties=p)
	for chunk_obj in chunk_objs:
		#print chunk_obj['id'], chunk_obj['coordinates'], chunk_obj['obj']
		idx.insert(chunk_obj['id'], chunk_obj['coordinates'], chunk_obj['obj'])	

	h5_fh.close()
	return idx


def create_2D_objs(var_id, dim_ids, chunk_objs):
	if var_id.chunks == None:
		print "variable is not chunked"
		sys.exit(1)
		
	chunk_dims = list(var_id.chunks)
	var_dims = list(var_id.shape)
	x = dim_ids[0]
	y = dim_ids[1]
	
	chunk_nums = []
	nchunks = 1
	for i in range(2):
		n = (var_dims[i] - 1) / chunk_dims[i] + 1
		chunk_nums.append(n)
		nchunks *= n

	for i in range(chunk_nums[0]):
		xmin = i * chunk_dims[0]
		xmax = min(var_dims[0] - 1, (i + 1) * chunk_dims[0] - 1)
		for j in range(chunk_nums[1]):
			ymin = j * chunk_dims[1]
			ymax = min(var_dims[1] - 1, (j + 1) * chunk_dims[1] - 1)
			chunk_obj = {}
			chunk_obj['id'] = i * chunk_nums[1] + j
			chunk_obj['coordinates'] = (x[xmin], y[ymin], x[xmax], y[ymax])
			#chunk_obj['obj'] = str(xmin) + ',' + str(ymin) + ':' + str(xmax) + ',' + str(ymax)
			chunk_obj['obj'] = (xmin, ymin, xmax, ymax)
			#print chunk_obj['id'], chunk_obj['coordinates'], chunk_obj['obj']
			#data = raw_input("")	
			chunk_objs.append(chunk_obj)


def search_index(h5path, variable, dimensions, bbox):
	# check if the idx data exists
	if !(os.path.isfile(h5path + '.idx')  and os.path.isfile(h5path + '.dat')):
		print 'index files don\'t exist'
		sys.exit(1)

	idx = rtree.Rtree(h5path)
	hits = list(idx.intersection(bbox, objects='raw'))
	#for item in hits:
	#	print item
	h5_fh = h5py.File(h5path, 'r')
	var_id = h5_fh[variable]
	for (xmin, ymin, xmax, ymax) in hits:
		print xmin, ymin, xmax, ymax
		print var_id[xmin:xmax,ymin:ymax]
	h5_fh.close()


def load_index(index_path):
	idx = rtree.Rtree(index_path)
	return idx


def test_create():
	dims = ['/y', '/x']
	idx = create_index(sys.argv[1], '/z', dims)
	#dump_index(idx, './rtree-index')
	#test_search(idx)


def test_search(idx):
	hits = list(idx.intersection((0, 0, 60, 60), objects=True))
	for item in hits:
		print item.id, item.bbox, item.object
	

def test_load():
	idx = rtree.Rtree('ETO_index')
	test_search(idx)

if __name__ == '__main__':
	test_create()
	#test_load()
