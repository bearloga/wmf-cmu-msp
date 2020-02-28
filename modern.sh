#!/bin/bash
kerberos-run-command analytics-privatedata hdfs dfs -ls /
klist
Rscript modern.R
