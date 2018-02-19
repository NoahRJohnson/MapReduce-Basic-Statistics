#!/usr/bin/env python

from mrjob.job import MRJob
import numpy as np
import os

'''
Calculate the variance in electricity prices among the states.

Requires 

Reads in a csv file with two variables per line, name of state and
price per kilowatt hour.

Requires two steps, one to calculate the mean, and one to
calculate the sample variance.
'''
class MRElecVar(MRJob):
        
    def mapper(self, _, line):
        name, pp_kwh = line.split(',')
        
        pp_kwh = float(pp_kwh)
        
        yield "Electricity_Variance", (name, pp_kwh)
        
    
    def reducer_init(self)
        with open("electricity_mean.tmp", "r") as f:
            self.mean =
        
        
    def reducer(self, key, values):
        nValues = 0
        total = 0
        
        for name, pp_kwh in values:
            total += pp_kwh
            nValues += 1
            
        avg = float(total) / nValues
        
        yield key, avg

if __name__ == '__main__':
    MRElecVar.run()