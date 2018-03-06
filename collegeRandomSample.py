#!/usr/bin/env python

from mrjob.job import MRJob
from mrjob.compat import jobconf_from_env
import numpy as np
import csv

'''
Obtain a random sample of approximately 100 colleges, in which
each college is equally likely to appear in the sample.

Reads in a csv file with columns:
College Name, State, Public (1)/ Private (2), Math SAT,
Verbal SAT, ACT, # appli. rec'd, # appl. accepted,
# new stud. enrolled, % new stud. from top 10%, 
% new stud. from top 25%, # FT undergrad, # PT undergrad,
in-state tuition,out-of-state tuition, room, board,
add. fees, estim. book costs, estim. personal $,
% fac. w/PHD, stud./fac. ratio, Graduation rate
'''
class MRCollegeRandomSample(MRJob):
        
    
    def mapper_init(self):
        self.numColleges = jobconf_from_env("my.job.settings.numColleges")
        
        self.numColleges = float(self.numColleges)

    def mapper(self, _, line):
        
        # Expected number of observations in our sample
        N_SAMPLE = 100
        
        # The probability we assign to each observation
        p = float(N_SAMPLE) / self.numColleges
        
        split_line = csv.reader([line])
        split_line = list(split_line)
        split_line = split_line[0]
        
        # Extract observation from csv line
        name, state, pub_priv, mathSAT, verbSAT, ACT, \
          numAppRec, numApplAcc, numNewStudEnrolled, \
          numStudTop10, numStudTop25, numFTunder, numPTunder, \
          inStateTuition, outStateTuition, room, board, \
          addFees, bookCosts, personalMoney, percFacPHD, \
          studFacRatio, gradRate = split_line
        
        # Yield this particular line, with probability p
        if np.random.uniform() < p:
            yield "_", name

if __name__ == '__main__':
    MRCollegeRandomSample.run()