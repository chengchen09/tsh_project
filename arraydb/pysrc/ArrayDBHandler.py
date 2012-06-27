import time

class ArrayDBHandler:
    def __init__(self):
	pass

    def executeQuery(self, request, data):
 
	print request
	s = raw_input()
	print s
	print 'return'
	return 'from server:executeQuery'
  
    def store(self, array, array_name):
      """
      Parameters:
       - array
       - array_name
      """
      pass
  
    def load_chunk(self, name, id, first, last):
      """
      Parameters:
       - name
       - id
       - first
       - last
      """
      pass
  
    def create_arrinfo(self, arrinfo):
	"""
	Parameters:
	- arrinfo
	"""
	s = raw_input()
	print s
	print 'return'
  
    def load(self, array_name):
      """
      Parameters:
       - array_name
      """
      pass
  
    def create_indexinfo(self, array_name, index_name, index_type):
      """
      Parameters:
       - array_name
       - index_name
       - index_type
      """
      pass
  
    def filter(self, array_name, var_name, min, max):
      """
      Parameters:
       - array_name
       - var_name
       - min
       - max
      """
      pass
  
    def set_fillvalue(self, chunk_name, fill_value):
      """
      Parameters:
       - chunk_name
       - fill_value
      """
      pass
  
    def set_para(self, key, value):
      """
      Parameters:
       - key
       - value
      """
      pass

    def test_nonblocking(self):
	print 'test nonblocking'
	#s = raw_input('please input')
	#print s
	time.sleep(10)
