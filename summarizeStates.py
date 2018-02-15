#!/usr/bin/python

from mrjob.job import MRJob

'''
Calculate the largest, smallest, and average (mean) population for a state. Calculate the
largest, smallest, and average (mean) area for a state.

Assumes a csv file input, with columns
Name, Abbreviation (Long), 2-Letter Abbreviation, Area (Sq. Miles), Population
'''
class MRSummarize(MRJob):

    def mapper(self, _, line):
        name, abr, area, pop = line.split()
        
        yield abr, (pop, area)
        
    def reducer(self, key, values):
        nValues = 0
        totalPop = 0
        totalArea = 0
        
        maxPop = 0
        maxArea = 0
        
        minPop = 0
        minArea = 0
        
        # this for-loop goes until all mappers finish
        for pop, area in values:
            totalPop += pop
            totalArea += area
            nValues += 1
            
            maxPop = max(maxPop, pop)
            maxArea = max(maxArea, area)
            
            minPop = min(minPop, pop)
            minArea = min(minArea, area)
            
        labels = "Largest Pop,Smallest Pop,Average Pop,Largest Area,Smallest Area,Average Area".split(",")
        d = dict(zip(labels, [maxPop, minPop, round(float(totalPop) / nValues, 2), maxArea, minArea, round(float(totalArea) / nValues, 2)]))
        yield key, d


if __name__ == '__main__':
    MRSummarize.run()