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

PUBLIC_COLLEGE_CODE = 1
PRIVATE_COLLEGE_CODE = 2

class MRCollegeStratifiedSample(MRJob):
        
    
    def mapper_init(self):
        self.numPubColleges = jobconf_from_env("my.job.settings.numPubColleges")
        self.numPrivColleges = jobconf_from_env("my.job.settings.numPrivColleges")
        
        self.numPubColleges = float(self.numPubColleges)
        self.numPrivColleges = float(self.numPrivColleges)

    def mapper(self, _, line):
        
        # Expected number of observations in our sample
        N_SAMPLE = 100
        
        '''
        The probability we assign to each college
        observation within its strata. This gives
        each observation within a particular strata
        equal probability of being sampled. And on
        average we expect there to be N_SAMPLE / 2
        sample observations from each strata, combining
        to give us a sample size of N_SAMPLE.
        '''
        p_pub = float(N_SAMPLE / 2) / self.numPubColleges
        p_priv = float(N_SAMPLE / 2) / self.numPrivColleges
        
        # Use csv library to split the line on commas
        # (the library handles nasty edge cases for us)
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
        
        pub_priv = int(pub_priv)
        
        if pub_priv == PUBLIC_COLLEGE_CODE:
            
            # Yield this particular line, with probability p_pub
            if np.random.uniform() < p_pub:
                yield "PUBLIC", name
                
        elif pub_priv == PRIVATE_COLLEGE_CODE:
            
            # Yield this particular line, with probability p_priv
            if np.random.uniform() < p_priv:
                yield "PRIVATE", name

        else:
            print line
            raise Exception("Invalid public/private data entry")

if __name__ == '__main__':
    MRCollegeStratifiedSample.run()