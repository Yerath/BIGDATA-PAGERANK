#!/bin/bash
make

rm -rf output/*

hadoop fs -copyToLocal /user/${USER}/pagerank/output_* output/

#echo "Original input:"
#hadoop fs -cat /user/${USER}/pagerank/output_0/*

#echo "Results:"
#hadoop fs -cat /user/${USER}/pagerank/output_total/*