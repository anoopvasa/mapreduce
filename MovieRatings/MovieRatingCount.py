'''Simplest MapReduce Program in Python using 
the package MRJob
Problem: Output a histogram of the ratings from a Movie Ratings dataset
Data: ml-100k\u.data from http://grouplens.org/
Author: anoop
'''

from mrjob.job import MRJob

class MRRatingCounter(MRJob):
    def mapper(self, key, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield rating, 1
    def reducer(self, rating, occurences):
        yield rating, sum(occurences)

if __name__ == '__main__':
    MRRatingCounter.run()
