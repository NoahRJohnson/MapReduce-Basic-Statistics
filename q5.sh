#!/usr/bin/env bash

# The output is '"College Counts" {"Total": x, "Public": y, "Private": z}'
tmp=$(python countColleges.py Example\ Data/colleges_no_header.csv)

# extract the different counts
totalCount=$(echo $tmp | cut -d' ' -f4 | tr -d ',')

pubCount=$(echo $tmp | cut -d' ' -f6 | tr -d ',')

privCount=$(echo $tmp | cut -d' ' -f8 | tr -d '}')

# Produce the random sample
python collegeRandomSample.py Example\ Data/colleges_no_header.csv --jobconf my.job.settings.numColleges=$(echo $totalCount)
