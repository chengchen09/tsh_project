#!/usr/bin/env python

x = [1, 2]

def func():
    global x
    x += [3, 4]
    print x

print x
func()
print x
