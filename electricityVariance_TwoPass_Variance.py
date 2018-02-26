#!/usr/bin/env python

from mrjob.job import MRJob
from mrjob.compat import jobconf_from_env
import numpy as np
import os

'''
Reads in a csv file with two variables per line, name of state and
price per kilowatt hour. Outputs the mean in electricity prices 
among the states. 

This is MapReduce job 1 of 2 necessary to calculate the variance
of electricity prices. This task is split up into two parts, to
allow a more numerically stable computation of the variance.
'''
class MRElecVar(MRJob):
    
    # Output the state name and price per kilowatt hour.
    def mapper(self, _, line):

        name, pp_kwh = line.split(',')
        
        pp_kwh = float(pp_kwh)
        
        yield "Electricity_Variance", pp_kwh
        
    # Load mean from first pass
    def reducer_init(self):
        self.mean = jobconf_from_env("my.job.settings.mean")
        self.mean = float(self.mean)
    
    '''
    Calculate variance in a numerically stable way.
    In this case our data makes up the full population,
    so we use divison by n, not (n-1).
    '''
    def reducer(self, key, values):
        nValues = 0
        total = 0
        
        for pp_kwh in values:
            total += (pp_kwh - self.mean)**2
            nValues += 1
            
        var = float(total) / nValues
        
        yield "Electricity_Variance", var

if __name__ == '__main__':
    MRElecVar.run()