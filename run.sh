#!/bin/bash
make

echo "Original input:"
hadoop fs -cat /user/${USER}/pagerank/output_0/*

echo "Results:"
hadoop fs -cat /user/${USER}/pagerank/output_total/*