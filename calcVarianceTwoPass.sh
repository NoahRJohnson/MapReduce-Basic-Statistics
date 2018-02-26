#!/usr/bin/env bash

# The output of TwoPass_Mean.py is 'key mean'
tmp=$(python electricityVariance_TwoPass_Mean.py Example\ Data/Electricity.csv)

# extract the mean
mean=$(echo $tmp | cut -d' ' -f2)

# Pass the mean as a parameter to the reducers in TwoPass_Variance.py
python electricityVariance_TwoPass_Variance.py Example\ Data/Electricity.csv --jobconf my.job.settings.mean=$(echo $mean)
