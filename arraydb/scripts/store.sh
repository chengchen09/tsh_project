#########################################################################
# Author: XXX
# Created Time: Wed 21 Mar 2012 01:37:35 PM CST
# File Name: store.sh
# Description: 
#########################################################################
#!/bin/bash

#../src/simple_client "store pres_128_lzma /home/bus/chen/data/pres.bin lzma"
#../src/simple_client "store pres_256_lzma /home/bus/chen/data/pres.bin lzma"
#../src/simple_client "store pres_512_lzma /home/bus/chen/data/pres.bin lzma"
#../src/simple_client "store pres_1024_lzma /home/bus/chen/data/pres.bin lzma"
#
#../src/simple_client "store etop_128_lzma /home/bus/chen/data/etopo1_bed_g_f4.flt lzma"
#../src/simple_client "store etop_256_lzma /home/bus/chen/data/etopo1_bed_g_f4.flt lzma"
#../src/simple_client "store etop_512_lzma /home/bus/chen/data/etopo1_bed_g_f4.flt lzma"
#../src/simple_client "store etop_1024_lzma /home/bus/chen/data/etopo1_bed_g_f4.flt lzma"

echo 128
../src/simple_client "store shum_128_lzma /home/bus/chen/data/shum.bin lzma"
echo 256
../src/simple_client "store shum_256_lzma /home/bus/chen/data/shum.bin lzma"
echo 512
../src/simple_client "store shum_512_lzma /home/bus/chen/data/shum.bin lzma"
echo 1024
#../src/simple_client "store shum_1024_lzma /home/bus/chen/data/shum.bin lzma"
