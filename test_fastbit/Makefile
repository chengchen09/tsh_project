
ALL: test_capi test_part

test_capi: test_capi.c
	gcc -o test_capi -Wall -g -O2 test_capi.c -I/home/bus/chen/local/include -L/home/bus/chen/local/lib -Wl,-rpath=/home/bus/chen/local/lib -lfastbit

test_part: cc_part.h test_part.cpp
	g++ -o test_part -Wall -g -O2 test_part.cpp -I/home/bus/chen/local/include -L/home/bus/chen/local/lib -Wl,-rpath=/home/bus/chen/local/lib -lfastbit
