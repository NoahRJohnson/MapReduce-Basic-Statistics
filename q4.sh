#!/usr/bin/env bash

# The output is '"Electricity_Mean" mean'
tmp=$(python electricityVariance_TwoPass_Mean.py Example\ Data/Electricity.csv)

# extract the average electricity usage
elecMean=$(echo $tmp | cut -d' ' -f2)

# The output is '"states" dict', where dict is a Python dictionary converted
# into a string.
tmp=$(python summarizeStates.py Example\ Data/states_clean.csv)

# extract the average area in square miles
avgArea=$(echo $tmp | cut -d' ' -f4 | tr -d ',')

# extract the average population size
avgPop=$(echo $tmp | cut -d' ' -f19 | tr -d ',')

# Pass the means as parameters to the reducers for OLS estimation
regr=$(python elecRegr.py Example\ Data/Electricity.csv Example\ Data/states_clean.csv --jobconf my.job.settings.elecMean=$(echo $elecMean) --jobconf my.job.settings.areaMean=$(echo $avgArea) --jobconf my.job.settings.popMean=$(echo $avgPop))

# Let the user look at the parameters
echo $regr

# Extract the intercepts and slopes for both regressions
interceptPop=$(echo $regr | cut -d' ' -f6 | tr -d '[,')
slopePop=$(echo $regr | cut -d' ' -f7 | tr -d '],')

interceptArea=$(echo $regr | cut -d' ' -f9 | tr -d '[,')
slopeArea=$(echo $regr | cut -d' ' -f10 | tr -d '],}')

rsquared=$(python elecRsq.py Example\ Data/Electricity.csv Example\ Data/states_clean.csv --jobconf my.job.settings.elecMean=$(echo $elecMean) --jobconf my.job.settings.areaIntercept=$(echo $interceptArea) --jobconf my.job.settings.popIntercept=$(echo $interceptPop) --jobconf my.job.settings.areaSlope=$(echo $slopeArea) --jobconf my.job.settings.popSlope=$(echo $slopePop))

echo $rsquared

rsqPop=$(echo $rsquared | cut -d' ' -f8 | tr -d ',')
rsqArea=$(echo $rsquared | cut -d' ' -f11 | tr -d '}')

echo $rsqPop $rsqArea | awk '{ if ($1 > $2) print "The model with population as a predictor is a better fit to the electricity price data."; else print "The model with area as a predictor is a better fit to the electricity price data." }'
