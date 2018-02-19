#!/usr/bin/env python

from mrjob.job import MRJob
import numpy as np

'''
Calculate the arithmetic mean of electricity prices among the states.

Reads in a csv file with two variables per line, name of state and
price per kilowatt hour.

Outputs the mean.
'''
class MRElecMean(MRJob):

    def mapper(self, _, line):
        name, pp_kwh = line.split(',')
        
        pp_kwh = float(pp_kwh)
        
        yield "Electricity_Mean", pp_kwh
        
    def reducer(self, key, values):
        nValues = 0
        total = 0
        
        for pp_kwh in values:
            total += pp_kwh
            nValues += 1
            
        avg = float(total) / nValues
        
        yield key, avg
        

if __name__ == '__main__':
    MRElecMean.run()