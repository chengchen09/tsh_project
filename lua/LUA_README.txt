1. 介绍
github address: https://github.com/chengchen09/json.git
模块的实现包括2个文件：json.lua，jsonparser.lua。其中json.lua是面向用户提供了操作Json格式数的API。API的具体数目如下：
json:
	load(f)：从file句柄中获得json格式的数据，返回lua table。
	loads(ss): 从python string中获得json格式的数据，返回lua table。
	load_python(f): 从file句柄中获得python格式的数据，返回lua table。
	dump(tt, f)：将table tt中的数据以json格式的数据写入到file f中。
	dumps(tt): 根据table tt中的数据返回json格式的string。
	dump_python(tt, f)：将table tt中的数据以lua格式的数据写入到file f中。

2. 测试文件
包括test.lua和test_pass1.lua，运行test.lua即可进行测试。测试会生成test_pass1.json, test_pass1.lj, test_pass2.json, test_pass2.lj四个文件