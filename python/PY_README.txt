1. 介绍
github address: https://github.com/chengchen09/json.git
类的实现包括2个文件：ccjson.py，jsonparser.py。其中ccjson中的类CCJson是面向用户提供了操作Json格式数的API。API的具体数目如下：
class CCJson:
	load(self, f)：从file句柄中获得json格式的数据。
	loads(self, ss): 从python string中获得json格式的数据。
	load_python(self, f): 从file句柄中获得python格式的数据。
	dump(self, f)：将json格式的数据写入到file f中。
	dumps(self): 返回json格式的python string。
	dump_python(self, f)：将json数据写入到file f中。
	update(self, d)：d为python的字典，用于更新json的数据信息。
	append(self, v)：用于对json的array进行append操作。
	提供了字典模式的读，写，删除以及keys()的功能。

2. 测试文件
包括test.py和test_pass1.py，运行test.py即可进行测试。测试会生成test_pass1.json, test_pass1.pj, test_pass2.json, test_pass2.pj四个文件

