#!/usr/bin/env bash

# The output is a '"states" dict', where dict is a Python dictionary converted
# into a string.
tmp=$(python summarizeStates.py Example\ Data/states_clean.csv)

# extract the average area in square miles
avgArea=$(echo $tmp | cut -d' ' -f4 | tr -d ',')

# extract the average population size
avgPop=$(echo $tmp | cut -d' ' -f19 | tr -d ',')

# Pass the means as parameters to the reducers for OLS estimation
python stateRegression.py Example\ Data/states_clean.csv --jobconf my.job.settings.areaMean=$(echo $avgArea) --jobconf my.job.settings.popMean=$(echo $avgPop)
