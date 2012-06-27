from unittest import TestCase
from ccjson import CCJson

JSON1 = r'''
[
    "JSON Test Pattern pass1",
    {"object with 1 member":["array with 1 element"]},
    {},
    [],
    -42,
    true,
    false,
    null,
    {
        "integer": 1234567890,
        "real": -9876.543210,
        "e": 0.123456789e-12,
        "E": 1.234567890E+34,
        "":  -23456789012E666,
        "zero": 0,
        "one": 1,
        "space": " ",
        "quote": "\"",
        "backslash": "\\",
        "controls": "\b\f\n\r\t",
        "slash": "/ & \/",
        "alpha": "abcdefghijklmnopqrstuvwyz",
        "ALPHA": "ABCDEFGHIJKLMNOPQRSTUVWYZ",
        "digit": "0123456789",
        "special": "`1~!@#$%^&*()_+-={':[,]}|;.</>?",
        "hex": "\u0123\u4567\u89AB\uCDEF\uabcd\uef4A",
        "true": true,
        "false": false,
        "null": null,
        "array":[  ],
        "object":{  },
        "address": "50 St. James Street",
        "url": "http://www.JSON.org/",
        "comment": "// /* <!-- --",
        "# -- --> */": " ",
        " s p a c e d " :[1,2 , 3

,

4 , 5        ,          6           ,7        ],
        "compact": [1,2,3,4,5,6,7],
        "jsontext": "{\"object with 1 member\":[\"array with 1 element\"]}",
        "\/\\\"\uCAFE\uBABE\uAB98\uFCDE\ubcda\uef4A\b\f\n\r\t`1~!@#$%^&*()_+-=[]{}|;:',./<>?"
: "A key can be any string"
    },
    0.5 ,98.6
,
99.44
,

1066


,"rosebud"]
'''

JSON2 = r'''
[[[[[[[[[[[[[[[[[[["Not too deep"]]]]]]]]]]]]]]]]]]]
'''

JSONS = [JSON1, JSON2]

fn = './test_pass'

class TestPass1(TestCase):
    def runTest(self):
        index = 0
        for JSON in JSONS:
            index = index + 1
            cj = CCJson()
            cj.loads(JSON)

            # json format
            jfile = fn + str(index) + '.json'
            f = open(jfile, 'w')
            cj.dump(f)
            f.close()
            jres = CCJson()
            f = open(jfile, 'r')
            jres.load(f)
            f.close()
            self.assertEquals(cj._dd, jres._dd)
            # python format
            pfile = fn + str(index) + '.pj'
            f = open(pfile, 'w')
            cj.dump_python(f)
            f.close()
            pres = CCJson()
            f = open(pfile, 'r')
            pres.load_python(f)
            f.close()
            self.assertEquals(cj._dd, pres._dd)
