fio -filename=/home/ubuntu/x/test-direct=1 -iodepth 8 -thread -rw=randwrite -ioengine=psync -bs=16k -size=500m -numjobs=30 -runtime=1000 -group_reporting -name=mytest
wrk -c 90 -t 4 --latency -d 30s -R700 -s bench.lua http://localhost:8081/put/3/ > 700
wrk -c 90 -t 4 --latency -d 30s -R1200 -s bench.lua http://localhost:8081/put/3/ > 1200
wrk -c 90 -t 4 --latency -d 30s -R1300 -s bench.lua http://localhost:8081/put/3/ > 1300
cat 700 1200 1300 | wrk2img output.png

