#!/usr/bin/python

from mrjob.job import MRJob
import numpy as np

'''
Calculate the largest, smallest, and average (mean) population for a state. Calculate the
largest, smallest, and average (mean) area for a state.

Assumes a csv file input, with columns
Name, Abbreviation (Long), 2-Letter Abbreviation, Area (Sq. Miles), Population
'''
class MRSummarize(MRJob):

    def mapper(self, _, line):
        name, abr_long, abr, area, pop = line.split(',')
        
        area = int(area)
        pop = int(pop)
        
        yield "states", (abr, pop, area)
        
    def reducer(self, key, values):
        nValues = 0
        totalPop = 0
        totalArea = 0
        
        maxPop = -1
        maxPopName = ""
        maxArea = -1
        maxAreaName = ""
        
        minPop = np.inf
        minPopName = ""
        minArea = np.inf
        minAreaName = ""
        
        # this for-loop goes until all mappers finish
        for name, pop, area in values:
            totalPop += pop
            totalArea += area
            nValues += 1
            
            if pop > maxPop:
                maxPop = pop
                maxPopName = name
            if area > maxArea:
                maxArea = area
                maxAreaName = name
            
            if pop < minPop:
                minPop = pop
                minPopName = name
            if area < minArea:
                minArea = area
                minAreaName = name
            
        avgPop = round(float(totalPop) / nValues, 2)
        avgArea = round(float(totalArea) / nValues, 2)
        
        vals = [str(n) + " (" + s + ")" for n,s in zip([maxPop, minPop, maxArea, minArea], [maxPopName, minPopName, maxAreaName, minAreaName])]
        vals.insert(2, avgPop)
        vals.append(avgArea)
        
        labels = "Largest Pop,Smallest Pop,Average Pop,Largest Area,Smallest Area,Average Area".split(",")
        d = dict(zip(labels, vals))
        
        yield key, d


if __name__ == '__main__':
    MRSummarize.run()