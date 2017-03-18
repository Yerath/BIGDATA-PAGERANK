#!/bin/bash
make

echo "Results:"
hadoop fs -cat /user/${USER}/pagerank/output/*