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

class MRCollegeCount(MRJob):

    def mapper(self, _, line):
        
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
        
        yield "_", pub_priv
        
    def reducer(self, key, values):
        
        pubCount = 0
        privCount = 0
        
        for public_private in values:
            if public_private == PUBLIC_COLLEGE_CODE:
                pubCount += 1
            elif public_private == PRIVATE_COLLEGE_CODE:
                privCount += 1
            else:
                raise Exception("Invalid public/private data entry")
                
        labels = ["Private", "Public", "Total"]
        
        d = dict(zip(labels, (privCount, pubCount, privCount + pubCount)))
                
        yield "College Counts", d

if __name__ == '__main__':
    MRCollegeCount.run()