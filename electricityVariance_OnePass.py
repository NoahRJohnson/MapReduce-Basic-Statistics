#!/usr/bin/env python

from mrjob.job import MRJob
from mrjob.step import MRStep
import numpy as np
import os

'''
Calculate the variance in electricity prices among the states.

Reads in a csv file with two variables per line, name of state and
price per kilowatt hour.

Uses the:

Var(X) = E(X^2) - E(X)^2

formula to calculate variance in one pass of the data. The sample
variance is estimated by using the sample mean of X in place of E(X).

WARNING: This method is numerically unstable, as the sum of X^2 and the
sum of X terms would not scale to big data levels. Catastrophic cancellation
could occur.
'''
class MRElecVar(MRJob):
        
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer_first),
            MRStep(reducer=self.reducer_second)
        ]
    
    def mapper(self, _, line):
        NUMBER_OF_FIRST_REDUCERS = 10
        
        name, pp_kwh = line.split(',')
        
        pp_kwh = float(pp_kwh)
        
        yield np.random.randint(1, NUMBER_OF_FIRST_REDUCERS), (name, pp_kwh)
        
    '''
    Calculate partial means
    '''
    def reducer_first(self, key, values):
        nValues = 0
        total = 0
        sqTotal = 0
        
        for name, pp_kwh in values:
            total += pp_kwh
            sqTotal += pp_kwh**2
            nValues += 1
            
        avg = float(total) / nValues
        sqAvg = float(sqTotal) / nValues
        
        yield "Electricity_Variance", (avg, sqAvg, nValues)
        
    '''
    Combine partial means into total mean.
    Then use total mean to compute sample
    variance.
    
    In this case our data is, 
    
    so we use divison by n, not (n-1).
    '''
    def reducer_second(self, key, values):
        n = 0
        total = 0
        sqTotal = 0
        
        for avg, sqAvg, count in values:
            tmp = count * avg
            sqTmp = count * sqAvg
            total += tmp
            sqTotal += sqTmp
            n += count
            
        e_x = float(total) / n
        
        e_x_Sq = float(sqTotal) / n
        
        var = e_x_Sq - e_x**2
        
        yield key, var


if __name__ == '__main__':
    MRElecVar.run()