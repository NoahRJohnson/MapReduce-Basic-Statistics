#!/usr/bin/env python

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env
import numpy as np

'''
Use linear regression to fit the following simple model:

Population = Area * <alpha> + <beta>

That is, find <alpha> and <beta> that minimize the squared residuals
when the state data is represented using this model.

Since this is OLS with a single predictor, we can use simple
computational formulas. These formulas require the mean for
the response and predictor. These need to be calculated first
and passed in via the jobconf parameter.

Reads in a csv file with columns:
Name, Abbreviation (Long), 2-Letter Abbreviation,
Area (Sq. Miles), Population.

Outputs the slope and intercept for a simple
linear regression of population on area.
'''
class MRStateRegr(MRJob):
        
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer_first),
            MRStep(reducer_init=self.reducer_init,
                   reducer=self.reducer_second)
        ]
    
    # Use random binning
    def mapper(self, _, line):
        NUMBER_OF_FIRST_REDUCERS = 10
        
        name, abr_long, abr, area, pop = line.split(',')
        
        area = int(area)
        pop = int(pop)
        
        yield np.random.randint(0, NUMBER_OF_FIRST_REDUCERS), (pop, area)
        
    # Load mean of response and predictor
    def reducer_init(self):
        self.yMean = jobconf_from_env("my.job.settings.popMean")
        self.xMean = jobconf_from_env("my.job.settings.areaMean")
        
        self.yMean = float(self.yMean)
        self.xMean = float(self.xMean)
        
    '''
    Calculate partial means
    '''
    def reducer_first(self, key, values):
        
        # both scaled by n
        partialCov = 0
        partialVar = 0
        
        for y, x in values:
            normalizedX = (x - self.xMean)
            normalizedY = (y - self.yMean)
            
            partialCov += normalizedX * normalizedY
            partialVar += normalizedX ** 2
        
        yield "_", (partialCov, partialVar)
        
    '''
    Combine partials into total.
    
    Then use totals to compute OLS estimates.
    '''
    def reducer_second(self, key, values):

        # both scaled by n
        totalCov = 0
        totalVar = 0
        
        for partialCov, partialVar in values:
            totalCov += partialCov
            totalVar += partialVar
            
        slope = totalCov / totalVar
        
        intercept = self.yMean - slope * self.xMean
        
        yield "Alpha, Beta", (slope, intercept)


if __name__ == '__main__':
    MRStateRegr.run()