'''
Author: Chen Cheng
Description: a class used to handle json
'''
from jsonparser import JsonParser, JsonParseError

def _jv_to_str(jv):
    '''transform the json value to a string'''
    ss = ''
    if type(jv) == type(''):
        ss = '"' + jv + '"'
    elif type(jv) == type({}):
        ss += '{'
        key_flag = False
        for key in jv.keys():
            if type(key) == type(''):
                key_flag = True
                ss += '"' + key + '":' + _jv_to_str(jv[key]) + ','
        if key_flag:
            ss = ss[:-1]
        ss += '}'
    elif type(jv) == type([]):
        ss += '['
        if len(jv) > 0:
            ss += _jv_to_str(jv[0])
        for i in range(1, len(jv)):
            ss += ',' + _jv_to_str(jv[i])
        ss += ']'
    elif jv == True:
        ss += 'true'
    elif jv == False:
        ss += 'false'
    elif jv == None:
        ss += 'null'
    else:
        ss = str(jv)
    return ss

def _jv_to_formatstr(jv, space=''):
    ss = ''
    if type(jv) == type(''):
        ss = '"' + jv + '"'
    elif type(jv) == type({}):
        ss += '{\n'
        space += '  '
        key_flag = False
        for key in jv.keys():
            if type(key) == type(''):
                key_flag = True
                ss += space + '"' + key + '": ' + _jv_to_formatstr(jv[key], space) + ',\n'
        space = space[:-2]
        if key_flag:
            ss = ss[:-2]
            ss += '\n' + space + '}'
        else:
            ss = ss[:-1] + '}'
    elif type(jv) == type([]):
        ss += '[\n'
        space += '  '
        if len(jv) > 0:
            ss += space + _jv_to_formatstr(jv[0], space)
        for i in range(1, len(jv)):
            ss += ',\n' + space + _jv_to_formatstr(jv[i], space)
        space = space[:-2]
        if len(jv) > 0:
            ss += '\n' + space + ']'
        else:
            ss = ss[:-1] + ']'
    elif jv == True:
        ss += 'true'
    elif jv == False:
        ss += 'false'
    elif jv == None:
        ss += 'null'
    else:
        ss = str(jv)
    return ss


class CCJson:
    def __init__(self):
        self._dd = None

    def load(self, f):
        ss = f.read()
        jparser = JsonParser(ss)
        self._dd = jparser.parse()
    
    def loads(self, ss):
        jparser = JsonParser(ss)
        self._dd = jparser.parse()
    
    def load_python(self, f):
        ss = f.read()
        jparser = JsonParser(ss)
        self._dd = jparser.parse()
    
    def dump(self, f):
        f.write(self._formatstr())
    
    def dumps(self):
        return self._formatstr()
    
    def dump_python(self, f):
        f.write(self._str())

    def update(self, d):
        '''update data from a python dictionary'''
        if type(self._dd) == type([]):
            keys = d.keys()
            keys.sort()
            for key in keys:
                self._dd[key] = d[key] 
        else:
            self._dd.update(d)

    def append(self, v):
        if type(self._dd) == type([]):
            self._dd.append(v)
        elif self._dd == None:
            self._dd = []
            self._dd.append(v)
        else:
            print "json object can't be append"


    def __setitem__(self, key, value):
        if self._dd == None:
            if type(key) == type(''):
                self._dd = {}
            elif type(key) == type(1):
                self._dd = []
            else:
                raise KeyError
        if (type(self._dd) == type([]) and type(key) != type(1)):
            raise IndexError
        elif type(self._dd) == type({}) and type(key) != type(''):
            raise KeyError
        self._dd[key] = value

    def __getitem__(self, key):
        return self._dd[key]

    def __delitem__(self, key):
        if type(self._dd) == type({}):
            del self._dd[key]
        elif type(self._dd) == type([]):
            self._dd.pop(key)
        else:
            raise KeyError
    
    def keys(self):
        if type(self._dd) == type({}):
            return self._dd.keys()
        elif type(self._dd) == type([]):
            return range(len(self._dd))
        else:
            return None

    def _str(self):
        return _jv_to_str(self._dd)
    
    def _formatstr(self):
        return _jv_to_formatstr(self._dd)
