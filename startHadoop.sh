#!/bin/bash
/usr/local/Cellar/hadoop/2.7.3/sbin/stop-yarn.sh
/usr/local/Cellar/hadoop/2.7.3/sbin/stop-dfs.sh

/usr/local/Cellar/hadoop/2.7.3/sbin/start-dfs.sh
/usr/local/Cellar/hadoop/2.7.3/sbin/start-yarn.sh
