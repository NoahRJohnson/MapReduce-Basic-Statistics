#!/usr/bin/env python

from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import jobconf_from_env
import numpy as np

'''
Which of the following linear models is a better fit for the electricity data
Electricity Price = Area * <alpha> + <beta>
Or
Electricity Price = Population * <alpha> + <beta>

Since this is OLS with a single predictor, we can use simple
computational formulas. These formulas require the mean for
the response and predictor. These need to be calculated first
and passed in via the jobconf parameter.

Reads in two csv files: Electricity.csv and states_clean.csv.

Electricity.csv has columns:
State, Price per Kilowatt Hour

States_clean.csv has columns:
Name, Abbreviation (Long), 2-Letter Abbreviation,
Area (Sq. Miles), Population.

The State column matches the Name column, so that is
the field we will join the tables on.

Outputs the slope and intercept for both regressions.
'''
class MRElecRegr(MRJob):
        
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.produce_observations),
            MRStep(reducer_init=self.reducer_init,
                   reducer=self.reducer_partials),
            MRStep(reducer_init=self.reducer_init,
                    reducer=self.reducer_final)
        ]
    
    # Spit out partial observations for each state
    def mapper(self, _, line):
        
        vals = line.split(',')
        
        if len(vals) > 2: # states.csv
            state_name, abr_long, abr, area, pop = vals
            
            area = int(area)
            pop = int(pop)
            
            yield state_name, (area, pop)

        else: # electricity.csv
            state_name, price = vals
            
            price = float(price)
        
            yield state_name, (price,)
        
    # Load mean of response and predictors
    def reducer_init(self):
        self.priceMean = jobconf_from_env("my.job.settings.elecMean")
        self.popMean = jobconf_from_env("my.job.settings.popMean")
        self.areaMean = jobconf_from_env("my.job.settings.areaMean")
        
        self.priceMean = float(self.priceMean)
        self.popMean = float(self.popMean)
        self.areaMean = float(self.areaMean)
        
    '''
    This reducer is more of a mapper, performing an INNER JOIN
    on the two input tables. Then it uses random binning to
    bin observations.
    '''
    def produce_observations(self, key, values):
        
        NUMBER_OF_FIRST_REDUCERS = 10
        
        price = None
        pop = None
        area = None
        
        for t in values:
            if len(t) == 1: # price
                price = t[0]
            elif len(t) == 2: # area, pop
                area = t[0]
                pop = t[1]
            else:
                raise Exception("Unrecognized mapper input in first reducer")
        
        # Raise an error if we don't have a full observation for this state
        assert(price is not None and pop is not None and area is not None)
        
        yield np.random.randint(0, NUMBER_OF_FIRST_REDUCERS), \
                (price, area, pop)
        
    '''
    Calculate partial means
    '''
    def reducer_partials(self, key, values):
        
        # covariance of predictors and response scaled by n
        partialAreaCov = 0
        partialPopCov = 0
        
        # variance of predictors scaled by n
        partialAreaVar = 0
        partialPopVar = 0
        
        for price, area, pop in values:
            normalizedPrice = (price - self.priceMean)
            normalizedArea = (area - self.areaMean)
            normalizedPop = (pop - self.popMean)
            
            partialAreaCov += normalizedArea * normalizedPrice
            partialPopCov += normalizedPop * normalizedPrice
            
            partialAreaVar += normalizedArea ** 2
            partialPopVar += normalizedPop ** 2
        
        yield "_", (partialAreaCov, partialPopCov, 
                        partialAreaVar, partialPopVar)
        
    '''
    Combine partials into total.
    
    Then use totals to compute OLS estimates.
    '''
    def reducer_final(self, key, values):

        # covariance of predictors and response scaled by n
        totalAreaCov = 0
        totalPopCov = 0
        
        # variance of predictors scaled by n
        totalAreaVar = 0
        totalPopVar = 0
        
        for partialAreaCov, partialPopCov, \
                partialAreaVar, partialPopVar in values:

            totalAreaCov += partialAreaCov
            totalPopCov += partialPopCov
            
            totalAreaVar += partialAreaVar
            totalPopVar += partialPopVar
            
        slopeArea = totalAreaCov / totalAreaVar
        
        slopePop = totalPopCov / totalPopVar
        
        interceptArea = self.priceMean - slopeArea * self.areaMean
        interceptPop = self.priceMean - slopePop * self.popMean
        
        labels = "Area, Pop".split(",")
        vals = [(interceptArea, slopeArea), (interceptPop, slopePop)]
        
        d = dict(zip(labels, vals))
        
        yield "Electricity Price Regressions", d


if __name__ == '__main__':
    MRElecRegr.run()