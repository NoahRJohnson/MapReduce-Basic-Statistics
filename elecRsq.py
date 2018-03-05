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

Reads in two csv files: Electricity.csv and states_clean.csv.

Electricity.csv has columns:
State, Price per Kilowatt Hour

States_clean.csv has columns:
Name, Abbreviation (Long), 2-Letter Abbreviation,
Area (Sq. Miles), Population.

The State column matches the Name column, so that is
the field we will join the tables on.

This script calculates R squared for both models, using
the definition:
R^2 = 1 - SS_Res / SS_Tot
'''
class MRElecRsq(MRJob):
        
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
        
    # Load mean of response, and regression coefficients
    def reducer_init(self):
        self.priceMean = jobconf_from_env("my.job.settings.elecMean")
        self.areaIntercept = jobconf_from_env("my.job.settings.areaIntercept")
        self.popIntercept = jobconf_from_env("my.job.settings.popIntercept")
        self.areaSlope = jobconf_from_env("my.job.settings.areaSlope")
        self.popSlope = jobconf_from_env("my.job.settings.popSlope")
        
        self.priceMean = float(self.priceMean)
        self.areaIntercept = float(self.areaIntercept)
        self.popIntercept = float(self.popIntercept)
        self.areaSlope = float(self.areaSlope)
        self.popSlope = float(self.popSlope)
        
        
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
    Calculate partial sums of squares
    '''
    def reducer_partials(self, key, values):
        
        partial_ss_res_area = 0
        partial_ss_res_pop = 0
        
        partial_ss_tot = 0
        
        for price, area, pop in values:
            
            partial_ss_tot += (price - self.priceMean)**2
            
            fArea = self.areaIntercept + self.areaSlope * area
            fPop = self.popIntercept + self.popSlope * pop
            
            eArea = fArea - price
            ePop = fPop - price
            
            partial_ss_res_area += eArea**2
            partial_ss_res_pop += ePop**2
        
        yield "_", (partial_ss_res_area, partial_ss_res_pop, 
                        partial_ss_tot)
        
    '''
    Combine partials into total.
    
    Then use totals to compute R^2.
    '''
    def reducer_final(self, key, values):

        ss_res_area = 0
        ss_res_pop = 0
        
        ss_tot = 0
        
        for partial_ss_res_area, partial_ss_res_pop, \
                        partial_ss_tot in values:

            ss_res_area += partial_ss_res_area
            ss_res_pop += partial_ss_res_pop
            
            ss_tot += partial_ss_tot
            
            
        rsqArea = 1 - ss_res_area / ss_tot
        rsqPop = 1 - ss_res_pop / ss_tot
        
        labels = "Area R^2, Pop R^2".split(",")
        vals = [rsqArea, rsqPop]
        
        d = dict(zip(labels, vals))
        
        yield "Electricity Price Regression Models", d


if __name__ == '__main__':
    MRElecRsq.run()